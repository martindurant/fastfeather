# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers

class Int(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsInt(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Int()
        x.Init(buf, n + offset)
        return x

    # Int
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Int
    def BitWidth(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

    # Int
    def IsSigned(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return bool(self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos))
        return False

def IntStart(builder): builder.StartObject(2)
def IntAddBitWidth(builder, bitWidth): builder.PrependInt32Slot(0, bitWidth, 0)
def IntAddIsSigned(builder, isSigned): builder.PrependBoolSlot(1, isSigned, 0)
def IntEnd(builder): return builder.EndObject()
