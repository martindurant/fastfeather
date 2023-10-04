from collections import namedtuple
import struct

# https://github.com/dvidelabs/flatcc/blob/master/doc/binary-format.md
# https://flatbuffers.dev/md__internals.html (briefer details)

# https://github.com/apache/arrow/blob/main/format

# this is a struct, not a Table. The middle value is int32, but alignment makes it int64
block = namedtuple("block", ("offset", "metaDataLength", "bodyLength"))("long", "long", "long")

key_value = {
    "key": "string",
    "value": "string"
}


class Union(dict):
    def __init__(self, bits):
        super().__init__(enumerate(bits))


class Enum(dict):
    def __init__(self, bits):
        super().__init__(enumerate(bits))


class NamedDict(dict):

    def __init__(self, data, name):
        super().__init__()
        self.update(data)
        self.name = name

    def __repr__(self):
        return f"{self.name} {super().__repr__()}"


version = Enum(("V1", "V2", "V3", "V4", "V5"))
field_node = namedtuple("node", ("length", "null_count"))("long", "long")

compression = Enum(("LZ4", "ZSTD"))

body_compression = {
    "codec": compression,
    "method": "BUFFER"
}

buffer = namedtuple("buffer", ("offset", "length"))("long", "long")

record_batch = NamedDict({
    "length": "long",
    "nodes": [field_node],
    "buffers": [buffer],
    "compression": body_compression,
    "variadic_counts": ["long"]
}, "record_batch")

dict_batch = NamedDict({
    "id": "long",
    "data": record_batch,
    "delta": "bool"
}, "dictionary_batch")

msg_header = Union(
    (
        None,
        None,  # Schema
        dict_batch,
        record_batch,
        None,  # Tensor
        None  # SparseTensor
    )
)

message = {
    "version": version,
    "header": msg_header,
    "body_length": "long",
    "metadata": [key_value]
}

Int = NamedDict({
  "bitWidth": "int",
  "is_signed": "bool"
}, "int")
FloatingPoint = NamedDict({
    "precision": Enum(("HALF", "SINGLE", "DOUBLE"))
}, "float")

Type = Union(
    (
        None,
        "NULL",  # a column where everything is None
        Int,
        FloatingPoint,
        "Binary",
        "Utf8",
        "Bool",
        "Decimal",
        "Date",
        "Time",
        "Timestamp",
        "Interval",
        "List",
        "Struct",
        "Union",
        "FixedSizeBinary",
        "FixedSizeList",
        "Map",
        "Duration",
        "LargeBinary",
        "LargeUtf8",
        "LargeList",
        "RunEndEncoded",
        "BinaryView",
        "Utf8View"
    ),
)


dictionary_encoding = {
  "id": "long",
  "indexType": Int,
  "isOrdered": "bool",
  "dictionaryKind": "enum"  # only one kind exists: dense
}

field = {
  "name": "string",
  "nullable": "bool",
  "type": Type,
  "dictionary": None,  # dictionary_encoding,
  "children": None,
  "custom_metadata": [key_value]
}
field["children"] = [field]

schema = {
  "endianness": "enum",
  "fields": [field],
  "custom_metadata": [key_value],
  "features": [Enum(("UNUSED", "DICTIONARY_REPLACEMENT", "COMPRESSED_BODY"))]
}

footer = {
  "version": version,
  "schema": schema,
  "dictionaries": [block],
  "recordBatches": [block],
  "custom_metadata": [key_value]
}


def parse_table(f, fieldspec: dict, root=False):
    if root:
        root_offset = int.from_bytes(f.read(4), "little")  # must be positive
        f.seek(root_offset - 4, 1)

    table0 = f.tell()  # start of TABLE
    voff = int.from_bytes(f.read(4), "little", signed=True)

    v0 = table0 - voff  # start of Vtable
    f.seek(v0)
    vsize = int.from_bytes(f.read(2), "little")
    tsize = int.from_bytes(f.read(2), "little")
    n_offsets = (vsize // 2) - 2
    field_offsets = [int.from_bytes(f.read(2), "little") for _ in range(n_offsets)]

    out = {}
    specs = iter(fieldspec.items())
    enum = False
    etype = None
    for off in field_offsets:

        if not enum:
            fieldname, fieldtype = next(specs)
        else:
            enum = False
        if off == 0 or fieldtype is None:
            # not present or explicitly skipped
            out[fieldname] = None
            # TODO: may have default value
            continue

        f.seek(table0 + off)  # find field

        # For unions of vector of unions, read only the type / vec of types, and
        # read the value(s) on the next offset iteration
        if isinstance(fieldtype, Union) and etype is None:
            etype = parse_inner(f, "enum")
            enum = True
        elif isinstance(fieldtype, list) and isinstance(fieldtype[0], Union) and etype is None:
            etype = parse_inner(f, ["enum"])
            enum = True
        else:
            out[fieldname] = parse_inner(f, fieldtype, etype=etype)
            etype = None
    for fieldname in fieldspec:
        if fieldname not in out:
            out[fieldname] = None

    return out


def parse_inner(f, fieldtype, etype=None):
        pos = f.tell()  # start of FIELD
        if fieldtype == "string":
            offset = int.from_bytes(f.read(4), "little")
            f.seek(pos + offset)
            ssize = int.from_bytes(f.read(4), "little")
            val = f.read(ssize)
        elif isinstance(fieldtype, Enum):
            return fieldtype[f.read(1)[0]]
        elif isinstance(fieldtype, list):
            # VECTOR; "Nesting vectors is not supported"
            offset = int.from_bytes(f.read(4), "little")
            f.seek(pos + offset)
            vecsize = int.from_bytes(f.read(4), "little")
            if isinstance(fieldtype[0], Union):
                # vector of tables, each a different union member according to etype
                oroff = [(f.tell(), int.from_bytes(f.read(4), "little")) for _ in range(vecsize)]
                val = []
                for (origin, offset), et in zip(oroff, etype):
                    f.seek(origin + offset)
                    v0 = parse_table(f, fieldtype[et])
                    v0["etype"] = fieldtype[et].name
                    val.append(v0)

            elif isinstance(fieldtype[0], Enum):
                val = [parse_inner(f, fieldtype[0]) for _ in range(vecsize)]

            elif isinstance(fieldtype[0], dict) or fieldtype[0] == "string":
                # vector of references (tables or strings)
                oroff = [(f.tell(), int.from_bytes(f.read(4), "little")) for _ in range(vecsize)]
                val = []
                for origin, offset in oroff:
                    f.seek(origin + offset)
                    val.append(parse_table(f, fieldtype[0]))
            else:
                # vector of values
                val = [parse_inner(f, fieldtype[0]) for _ in range(vecsize)]
        elif isinstance(fieldtype, Union):
            # should always be a collection of tables
            if isinstance(fieldtype[etype], dict):
                offset = int.from_bytes(f.read(4), "little")
                f.seek(pos + offset)
                val = parse_table(f, fieldtype[etype])
                val["etype"] = fieldtype[etype].name
            else:
                # should not happen
                val = fieldtype[etype]
        elif isinstance(fieldtype, dict):
            # TABLE (not struct)
            offset = int.from_bytes(f.read(4), "little")
            f.seek(pos + offset)
            val = parse_table(f, fieldtype)
        elif hasattr(fieldtype, "_asdict"):  # named tuple convention
            # STRUCTs, always inlined; "Structs may only contain scalars or other structs"
            val = type(fieldtype)(*(parse_inner(f, _) for _ in fieldtype))
        elif fieldtype in ["enum", "byte"]:
            # simple enum: just get the value
            # NB: an enum can in theory be longer than one byte, but have never seen this
            val = f.read(1)[0]
        elif fieldtype == "bool":
            val = f.read(1)[0] == 1
        elif fieldtype == "short":
            val = int.from_bytes(f.read(2), "little")
        elif fieldtype == "int":
            val = int.from_bytes(f.read(4), "little")
        elif fieldtype == "long":
            val = int.from_bytes(f.read(8), "little")
        elif fieldtype == "float":
            val = struct.unpack('f', f.read(4))[0]
        elif fieldtype == "double":
            val = struct.unpack('d', f.read(8))[0]
        else:
            val = fieldtype  # literal value
        return val


def parse_feather(infile):
    """Main entry point: get schema and offsets from file footer"""
    infile.seek(-10, 2)
    size = int.from_bytes(infile.read(4), "little")
    assert infile.read() == b"ARROW1"
    infile.seek(-size - 10, 2)
    return parse_table(infile, footer, root=True)


def parse_batch(infile, body_size, meta_size, offset):
    assert meta_size % 8 == 0
    infile.seek(offset)
    assert infile.read(4) == b"\xff\xff\xff\xff"
    meta_length = int.from_bytes(infile.read(4), "little")
    assert meta_length == meta_size - 8
    return parse_table(infile, message, root=True)
