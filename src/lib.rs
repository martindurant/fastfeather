use std::collections::{HashMap, VecDeque};
use pyo3::prelude::*;
use std::io::{self, Cursor, Error, ErrorKind, Read, Seek, SeekFrom};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use std::sync::Arc;
use std::fmt;
use std::slice;
use std::fmt::{Display, Formatter};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyAttributeError, PyIndexError};

#[macro_use]
extern crate lazy_static;


#[derive(Clone, Debug)]
enum FlatTypes {
    None,
    Bool,
    Short,
    Int,
    Long,
    Strin,
    Float,
    Double,
    Enum(Vec<&'static str>),
    Struct,
    Table(&'static str),  // link to table of given name
    Union,
    Value(&'static str)  // values e.g., for ENUM
}

#[derive(Clone, Debug)]
enum ChildType {
    Simple(FlatTypes),
    List(FlatTypes),
    Stru(Vec<FlatTypes>),  // only simple types allowed in flat struct
    Un(Vec<FlatTypes>),  // flat union
}

lazy_static! {
    static ref SCHEMAS: HashMap<&'static str, (Vec<ChildType>, Vec<&'static str>)> = {
        let mut h: HashMap<&str, (Vec<ChildType>, Vec<&str>)> = HashMap::with_capacity(20);
        h.insert("key_value",
            (vec!(
                ChildType::Simple(FlatTypes::Strin),
                ChildType::Simple(FlatTypes::Strin)
            ),
                vec!("key", "value")
            )
        );
        h.insert("int",
            (
                vec!(ChildType::Simple(FlatTypes::Int),
                ChildType::Simple(FlatTypes::Bool)
            ),
                vec!("bitWidth", "is_signed")
            )
        );
        h.insert("float",
            (
                vec!(ChildType::Simple(FlatTypes::Enum(vec!("HALF", "SINGLE", "DOUBLE")))
            ),
                vec!("precision")
        ));
        let typ = vec!(  // feather schema types, not flat types
            FlatTypes::Value("NULL"), //  # a column where everything is None
            FlatTypes::Table("int"),
            FlatTypes::Table("float"),
            FlatTypes::Value("Binary"),
            FlatTypes::Value("UTF8"),
            FlatTypes::Value("Bool"),
            FlatTypes::Value("Decimal"),
            FlatTypes::Value("Date"),
            FlatTypes::Value("Time"),
            FlatTypes::Value("Timestamp"),
            FlatTypes::Value("Interval"),
            FlatTypes::Value("List"),
            FlatTypes::Value("Struct"),
            FlatTypes::Value("Union"),
            FlatTypes::Value("FixedSizeBinary"),
            FlatTypes::Value("FixedSizeList"),
            FlatTypes::Value("Map"),
        );
        h.insert("field",
            (
                vec!(
                    ChildType::Simple(FlatTypes::Strin),
                    ChildType::Simple(FlatTypes::Bool),
                    ChildType::Un(typ),
                    ChildType::Simple(FlatTypes::None),
                    ChildType::Simple(FlatTypes::None),
                    ChildType::List(FlatTypes::Table("key_value")),
                    ChildType::List(FlatTypes::Table("field"))
            ),
                vec!("name", "nullable", "type", "dictionary", "children", "custom_metadata")

        ));
        h.insert("schema",
            (
                vec!(
                    ChildType::Simple(FlatTypes::Enum(vec!("Little", "Big"))),
                    ChildType::List(FlatTypes::Table("field")),
                    ChildType::List(FlatTypes::Table("key_value")),
                    ChildType::List(FlatTypes::Enum(vec!("UNUSED", "DICTIONARY_REPLACEMENT", "COMPRESSED_BODY")))
            ),
                vec!("endianness", "fields", "custom_metadata", "features")
        ));
        h.insert("footer",
            (
                vec!(
                    ChildType::Simple(FlatTypes::Enum(vec!("V1", "V2", "V3", "V4", "V5"))),
                    ChildType::Simple(FlatTypes::Table("schema")),
                    ChildType::Simple(FlatTypes::None), //dictionaries
                    ChildType::Simple(FlatTypes::None), //record batches
                    ChildType::List(FlatTypes::Table("key_value"))
            ),
                vec!("version", "schema", "dictionaries", "record_batches", "custom_metadata")
        ));
        h
    };
}

#[pyclass]
#[derive(Clone)]
struct FlatTable {
    name: String,
    schema: &'static (Vec<ChildType>, Vec<&'static str>),
    buf: Arc<Vec<u8>>,
    offset: usize,
    offsets: Vec<i16>,
}

#[derive(Clone)]
enum OutTypes {
    Bool(bool),
    Str(String),
    Number(i64),
    Fumber(f64),
    Flat(FlatTable),
    Empty
}

enum InnerGet {
    Simple(OutTypes),
    List(Vec<OutTypes>)
}

impl IntoPy<PyObject> for OutTypes {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self.clone() {
            OutTypes::Bool(b) => b.into_py(py),
            OutTypes::Str(s) => s.into_py(py),
            OutTypes::Number(x) => x.into_py(py),
            OutTypes::Fumber(x) => x.into_py(py),
            OutTypes::Flat(ft) => ft.into_py(py),
            OutTypes::Empty => ().into_py(py)
        }
    }
}


// No-copy python buffer-like (e.g., bytes) to u8 slice (must not outlive original)
#[inline(always)]
fn py_to_byteslice(value: &PyAny) -> &'static mut [u8] {
    let buf: PyBuffer<u8> = value.extract().unwrap();
    unsafe {
        slice::from_raw_parts_mut(buf.buf_ptr() as *mut u8, buf.len_bytes())
    }
}


#[derive(Debug)]
struct MyError {}

impl Display for MyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}
impl std::error::Error for MyError {}

impl FlatTable { 
    pub fn new(name: String, buf: Arc<Vec<u8>>, offset: usize) -> Self {
        let schema = SCHEMAS.get(name.as_str()).unwrap();
        let voff = LittleEndian::read_i32(&buf[offset .. offset + 4]);
        let off_voff = (offset as i32 - voff) as usize;
        let vsize = LittleEndian::read_u16(&buf[off_voff .. off_voff + 2]);
        let noffsets = (vsize / 2) - 2;
        let offsets: Vec<i16> =
            (off_voff + 4 .. off_voff + 4 + noffsets as usize * 2)
            .step_by(2)
            .map(|x| LittleEndian::read_i16(&buf[x .. x + 2]))
            .collect();
        Self { name, schema, buf, offset, offsets }
    }
    fn get_simple_type(&self, offset: usize, typ: &FlatTypes) -> OutTypes {
        match typ {
            FlatTypes::Strin => {
                let off = LittleEndian::read_u32(&self.buf[offset .. offset + 4]) as usize;
                let ssize = LittleEndian::read_u32(&self.buf[offset + off .. offset + off + 4]) as usize;
                OutTypes::Str(std::str::from_utf8(
                    // UTF8 parse error possible here
                    &self.buf[offset + off + 4 .. offset + off + 4 + ssize]).unwrap().to_string())
            },
            FlatTypes::Enum(v) => {
                let choice = self.buf[offset] as usize;
                OutTypes::Str(v[choice].to_string())
            },
            FlatTypes::Table(name) => {
                let toff = LittleEndian::read_u32(&self.buf[offset .. offset + 4]) as usize;
                OutTypes::Flat(FlatTable::new(name.to_string(), self.buf.clone(), offset + toff))
            },
            FlatTypes::Bool => {
                OutTypes::Bool(self.buf[offset] > 0)
            },
            FlatTypes::Short => {
                OutTypes::Number(LittleEndian::read_i16(&self.buf[offset .. offset + 2]) as i64)
            }
            FlatTypes::Int => {
                OutTypes::Number(LittleEndian::read_i32(&self.buf[offset .. offset + 4]) as i64)
            }
            FlatTypes::Long => {
                OutTypes::Number(LittleEndian::read_i64(&self.buf[offset .. offset + 8]))
            }
            FlatTypes::Float => {
                OutTypes::Fumber(LittleEndian::read_f32(&self.buf[offset .. offset + 4]) as f64)
            }
            FlatTypes::Double => {
                OutTypes::Fumber(LittleEndian::read_f64(&self.buf[offset .. offset + 8]))
            }
            FlatTypes::Value(s) => OutTypes::Str(s.to_string()),
            _ => OutTypes::Empty
        }
    }

    fn get_union(&self, choice_offset: usize, value_offset: usize, typ: &[FlatTypes]) -> OutTypes {
        let typ = &typ[self.buf[choice_offset] as usize];
        self.get_simple_type(value_offset, typ)
    }

    fn get_list(&self, offset: usize, typ: &FlatTypes) -> Vec<OutTypes> {
        let off = LittleEndian::read_u32(&self.buf[offset .. offset + 4]) as usize;
        let size = LittleEndian::read_u32(&self.buf[offset + off .. offset + off + 4]) as usize;
        let el_size: usize = match typ {
            FlatTypes::Bool | FlatTypes::Enum(_) => 1,
            FlatTypes::Short => 2,
            FlatTypes::Int | FlatTypes::Strin | FlatTypes::Table(_) | FlatTypes::Float => 4,
            FlatTypes::Long | FlatTypes::Double => 8,
            _ => 0
        };
        (0..size).map(|i| {
            self.get_simple_type(off + offset + 4 + i * el_size, typ)
        }).collect()
    }

    fn get_inner(&self, mut val: usize) -> Result<InnerGet, MyError> {
        let typ: &ChildType = self.schema.0.get(val as usize).expect("");
        for i in 0..val {
            match self.schema.0.get(i) {
                Some(ChildType::Simple(FlatTypes::Union)) => {val += 1;},
                Some(ChildType::List(FlatTypes::Union)) => {val += 1;},
                _ => ()
            }
        }
        let off = self.offsets[val] as usize;
        if off == 0 {
            return Err(MyError {})
        }
        match typ {
            ChildType::Simple(x) =>
                Ok(InnerGet::Simple(self.get_simple_type(self.offset + off, x))),
            ChildType::List(typ) =>
                Ok(InnerGet::List(self.get_list(self.offset + off, typ))),
            ChildType::Un(typ) => {
                let off2 = self.offsets[val + 1] as usize;
                Ok(InnerGet::Simple(self.get_union(self.offset + off, self.offset + off2, typ)))
            }
            _ => Err(MyError {})
        }
    }
}


impl fmt::Debug for FlatTable {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct(&format!("Table '{}'\n", self.name))
            .field("schema", &self.schema)
            .field("offset", &self.offset)
            .field("offsets", &format_args!("{:?}", self.offsets))
            .finish()
    }
}

#[pymethods]
impl FlatTable {
    #[new]
    fn newtest<'py>(_py: Python, name: String, buf: &PyAny, offset: usize) -> Self {
        FlatTable::new(name, Arc::new(py_to_byteslice(buf).to_vec()), offset)
    }

    fn __str__<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(format!("{:?}", self).into_py(py))
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(format!("<FlatTable '{}'>", self.name).into_py(py))
    }

    fn field_name(&self, num: usize) -> PyResult<String> {
        Ok(self.schema.1.get(num).unwrap_or((&"None").into()).to_string())
    }

    fn n_fields(&self) -> PyResult<usize> {
        Ok(self.schema.0.len())
    }

    fn __getattr__<'py>(&self, py:Python<'py>, name: &str) -> PyResult<PyObject> {
        let ind =self.schema.1.iter().position(|field| field.eq(&name));
        match ind {
            Some(x) => self.__getitem__(py, x),
            None => Err(PyAttributeError::new_err(name.to_string()))
        }
    }

    fn __getitem__<'py>(&self, py: Python<'py>, val: usize) -> PyResult<PyObject> {
        let out = self.get_inner(val);
        match out {
            Ok(InnerGet::Simple(x)) => Ok(x.into_py(py)),
            Ok(InnerGet::List(x)) => Ok(x.into_py(py)),
            Err(_) => Ok(().into_py(py))
        }
    }
}

fn parse_feather<I: Read + Seek>(mut reader: I, root: bool) -> io::Result<FlatTable>{
    reader.seek(SeekFrom::End(-10))?;
    let size: i32 = reader.read_i32::<LittleEndian>()?;
    reader.seek(SeekFrom::End(-size as i64 - 10))?;
    let off = if root {reader.read_i32::<LittleEndian>()? - 4} else {0} as usize;
    let mut s: Vec<u8> = Vec::with_capacity((size as usize) + 10);
    reader.read_to_end(&mut s)?;
    match s[(size as usize)..].eq(&Vec::from("ARROW1")) {
        true => Ok(FlatTable::new("footer".to_string(), Arc::new(s), off)),
        false => Err(Error::new(ErrorKind::InvalidData, "Signature not found"))
    }
}

#[pyfunction]
fn py_footer(buf: &PyAny) -> PyResult<FlatTable> {
    let cur: Cursor<&mut [u8]> = Cursor::new(py_to_byteslice(buf));
    Ok(parse_feather(cur, true)?)
}

#[pymodule]
fn fastfeather(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FlatTable>()?;
    m.add_function(wrap_pyfunction!(py_footer, m)?)?;
    Ok(())
}
