# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers

# /// ----------------------------------------------------------------------
# /// Data structures for dense tensors
# /// Shape data for a single axis in a tensor
class TensorDim(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsTensorDim(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = TensorDim()
        x.Init(buf, n + offset)
        return x

    # TensorDim
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

# /// Length of dimension
    # TensorDim
    def Size(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

# /// Name of the dimension, optional
    # TensorDim
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

def TensorDimStart(builder): builder.StartObject(2)
def TensorDimAddSize(builder, size): builder.PrependInt64Slot(0, size, 0)
def TensorDimAddName(builder, name): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def TensorDimEnd(builder): return builder.EndObject()
