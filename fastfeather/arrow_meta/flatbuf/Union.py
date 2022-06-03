# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers

# /// A union is a complex type with children in Field
# /// By default ids in the type vector refer to the offsets in the children
# /// optionally typeIds provides an indirection between the child offset and the type id
# /// for each child `typeIds[offset]` is the id used in the type vector
class Union(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsUnion(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Union()
        x.Init(buf, n + offset)
        return x

    # Union
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Union
    def Mode(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int16Flags, o + self._tab.Pos)
        return 0

    # Union
    def TypeIds(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int32Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return 0

    # Union
    def TypeIdsAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # Union
    def TypeIdsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

def UnionStart(builder): builder.StartObject(2)
def UnionAddMode(builder, mode): builder.PrependInt16Slot(0, mode, 0)
def UnionAddTypeIds(builder, typeIds): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(typeIds), 0)
def UnionStartTypeIdsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def UnionEnd(builder): return builder.EndObject()
