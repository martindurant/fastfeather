

from .flatbuf import *

union_maps = {
    "Header": MessageHeader,
    "Type": Type
}


def getname(obj, name):
    out = getattr(obj, name)
    if hasattr(obj, name + "Type"):
        union = union_maps[name]
        unique = getattr(obj, name + "Type")()
        val = [member for member in dir(union) if getattr(union, member, -1) == unique][0]
        cls = globals()[val]
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
            if name.endswith(special) and name[:-len(special)]:
                needed = False
                break
        if not needed:
            continue
        bit = getname(obj, name)
        if name.endswith("Type") and name[:-len("Type")]:
            union = union_maps[name[:-len("Type")]]
            bit = str([member for member in dir(union) if getattr(union, member, -1) == bit][0])
        if bit and isinstance(bit, list) and hasattr(bit[0], "Init"):
            bit = [to_dict(_) for _ in bit]
        elif hasattr(bit, "Init"):
            bit = to_dict(bit)
        out[name] = bit
    return out


def parse_from_file(bytestring, cls):
    methname = [_ for _ in dir(cls) if _.startswith("GetRootAs")][0]
    return getattr(cls, methname)(bytestring, 0)


def test():
    f = open("org/apache/arrow/flatbuf/test.feather", "rb")
    assert f.read(12) == b"ARROW1\x00\x00\xff\xff\xff\xff"  # plus alignment padding plus "continuation" 0xff
    size = int.from_bytes(f.read(4), "little")
    mes = parse_from_file(f.read(size), Message)
    return mes
