import cramjam

from .flatbuf import *

union_maps = {
    "Header": MessageHeader,
    "Type": Type
}
NONE = None


def getname(obj, name):
    out = getattr(obj, name)
    if hasattr(obj, name + "Type"):
        union = union_maps[name]
        unique = getattr(obj, name + "Type")()
        val = [member for member in dir(union) if getattr(union, member, -1) == unique][0]
        cls = globals()[val]
        if cls is None:
            return None
        inst = cls()
        inst.Init(out().Bytes, out().Pos)
        return inst
    if hasattr(obj, name + "Length"):
        return [out(i) for i in range(getattr(obj, name + "Length")())]
    return out()


def to_dict(obj):
    names = {_ for _ in dir(obj) if _[0].isupper() and _ != "Init" and "GetRootAs" not in _}
    out = {}
    # sort to ensure Field happens before FieldType
    for name in sorted(names):
        needed = True
        for special in ("Length", "AsNumpy"):
            if name.endswith(special) and name[:-len(special)] in names:
                needed = False
                break
        if not needed:
            continue
        bit = getname(obj, name)
        if name.endswith("Type") and name[:-len("Type")] and name != "IndexType":
            # NB IndexType is a normal field, breaking the union description convention
            union = union_maps[name[:-len("Type")]]
            bit = str([member for member in dir(union) if getattr(union, member, -1) == bit][0])
        if bit and isinstance(bit, list) and hasattr(bit[0], "Init"):
            bit = [to_dict(_) for _ in bit]
        elif hasattr(bit, "Init"):
            bit = to_dict(bit)
        out[name] = bit
    return out


def parse_from_bytes(bytestring, cls):
    methname = [_ for _ in dir(cls) if _.startswith("GetRootAs")][0]
    return getattr(cls, methname)(bytestring, 0)


def parse_feather(infile):
    """Main entry point: get schema and offsets from file footer"""
    infile.seek(-10, 2)
    size = int.from_bytes(infile.read(4), "little")
    assert infile.read() == b"ARROW1"
    infile.seek(-size - 10, 2)
    bytestring = infile.read(size)
    flb = parse_from_bytes(bytestring, Footer)
    return to_dict(flb)


codecs = {0: cramjam.lz4.decompress, 1: cramjam.zstd.decompress}


def parse_batch(infile, BodyLength, MetaDataLength, Offset, buffers=True):
    offset, meta_size, body_size = Offset, MetaDataLength, BodyLength
    assert meta_size % 8 == 0
    infile.seek(offset)
    meta = infile.read(meta_size)
    assert meta[:4] == b"\xff\xff\xff\xff"
    meta_length = int.from_bytes(meta[4:8], "little")
    assert meta_length == meta_size - 8
    body = infile.read(body_size)
    msg = to_dict(parse_from_bytes(meta[8:], Message))  # 8 bytes for start of meta
    if msg['HeaderType'] == 'RecordBatch':
        part = msg["Header"]
    elif msg['HeaderType'] == 'DictionaryBatch':
        part = msg["Header"]["Data"]

    bufs = []
    if buffers:
        codec = codecs[part["Compression"]["Codec"]]
        for buf_met in part["Buffers"]:
            bdata = body[buf_met["Offset"]:buf_met["Offset"] + buf_met["Length"]]
            blen = int.from_bytes(bdata[:8], "little")
            uncomp = bytes(codec(bdata[8:])) if blen > 0 else bdata[8:]
            bufs.append(uncomp)
    return part, bufs
