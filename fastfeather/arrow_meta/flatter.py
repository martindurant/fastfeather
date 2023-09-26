import enum
from collections import namedtuple
import struct

# https://github.com/dvidelabs/flatcc/blob/master/doc/binary-format.md
# https://flatbuffers.dev/md__internals.html (briefer details)

# this is a struct, not a Table. The middle value is int32, but alignment makes it int64
block = namedtuple("block", ("offset", "metaDataLength", "bodyLength"))("long", "long", "long")

key_value = {
    "key": "string",
    "value": "string"
}


class Type(enum.Enum):  # actually a Union with many of these being Tables of their own
    Null = 0
    Int = 1
    FloatingPoint = 2
    Binary = 3
    Utf8 = 4
    Bool = 5
    Decimal = 6
    Date = 7
    Time = 8
    Timestamp = 9
    Interval = 10
    List = 11
    Struct_ = 12
    Union = 13
    FixedSizeBinary = 14
    FixedSizeList = 15
    Map = 16
    Duration = 17
    LargeBinary = 18
    LargeUtf8 = 19
    LargeList = 20
    RunEndEncoded = 21
    BinaryView = 22
    Utf8View = 23


Int = {
  "bitWidth": "int",
  "is_signed": "bool"
}

dictionary_encoding = {
  "id": "long",
  "indexType": Int,
  "isOrdered": "bool",
  "dictionaryKind": "enum"  # only one kind exists: dense
}

field = {
  "name": "string",
  "nullable": "bool",
  "type": "enum",  # Type
  "dictionary": None,  # dictionary_encoding,
  "children": None,
  "custom_metadata": [key_value]
}
field["children"] = [field]

schema = {
  "endianness": "enum",
  "fields": [field],
  "custom_metadata": [key_value],
  "features": ["enum"]
}

footer = {
  "version": "enum",
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
    n_offsets = min((vsize // 2) - 2, len(fieldspec))
    field_offsets = [int.from_bytes(f.read(2), "little") for _ in range(n_offsets)]
    field_offsets.extend([0] * (len(fieldspec) - len(field_offsets)))

    out = {}
    for off, (fieldname, fieldtype) in zip(field_offsets, fieldspec.items()):
        if off == 0 or fieldtype is None:
            # not present or explicitly skipped
            out[fieldname] = None
            # TODO: may have default value
            continue
        if fieldname == "dictionary":
            breakpoint()
        f.seek(table0 + off)  # find field
        out[fieldname] = parse_inner(f, fieldtype)

    return out


def parse_inner(f, fieldtype):
        pos = f.tell()  # start of FIELD
        if fieldtype == "string":
            offset = int.from_bytes(f.read(4), "little")
            f.seek(pos + offset)
            ssize = int.from_bytes(f.read(4), "little")
            val = f.read(ssize)
        elif isinstance(fieldtype, list):
            # VECTOR; "Nesting vectors is not supported"
            offset = int.from_bytes(f.read(4), "little")
            f.seek(pos + offset)
            vecsize = int.from_bytes(f.read(4), "little")
            if isinstance(fieldtype[0], (dict, enum.Enum)) or fieldtype[0] == "string":
                # vector of references
                oroff = [(f.tell(), int.from_bytes(f.read(4), "little")) for _ in range(vecsize)]
                val = []
                for origin, offset in oroff:
                    f.seek(origin + offset)
                    val.append(parse_table(f, fieldtype[0]))
            else:
                # vector of values
                val = [parse_inner(f, fieldtype[0]) for _ in range(vecsize)]
        elif isinstance(fieldtype, dict):
            # TABLE (not struct)
            offset = int.from_bytes(f.read(4), "little")
            f.seek(pos + offset)
            val = parse_table(f, fieldtype)
        elif hasattr(fieldtype, "_asdict"):  # named tuple convention
            # STRUCTs, always inlined; "Structs may only contain scalars or other structs"
            val = type(fieldtype)(*(parse_inner(f, _) for _ in fieldtype))
        elif isinstance(fieldtype, enum.Enum):
            etype = f.read(1)[0]
            if etype == 0:
                val = None
            # TODO: evaluate enum subtypes
        elif fieldtype in ["enum", "byte"]:
            # simple enum: just get the value
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
        return val


def parse_feather(infile):
    """Main entry point: get schema and offsets from file footer"""
    infile.seek(-10, 2)
    size = int.from_bytes(infile.read(4), "little")
    assert infile.read() == b"ARROW1"
    infile.seek(-size - 10, 2)
    return parse_table(infile, footer, root=True)
