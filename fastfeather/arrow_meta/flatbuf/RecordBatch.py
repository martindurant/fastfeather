# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers

# /// A data header describing the shared memory layout of a "record" or "row"
# /// batch. Some systems call this a "row batch" internally and others a "record
# /// batch".
class RecordBatch(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsRecordBatch(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = RecordBatch()
        x.Init(buf, n + offset)
        return x

    # RecordBatch
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

# /// number of records / rows. The arrays in the batch should all have this
# /// length
    # RecordBatch
    def Length(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

# /// Nodes correspond to the pre-ordered flattened logical schema
    # RecordBatch
    def Nodes(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 16
            from .FieldNode import FieldNode
            obj = FieldNode()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # RecordBatch
    def NodesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

# /// Buffers correspond to the pre-ordered flattened buffer tree
# ///
# /// The number of buffers appended to this list depends on the schema. For
# /// example, most primitive arrays will have 2 buffers, 1 for the validity
# /// bitmap and 1 for the values. For struct arrays, there will only be a
# /// single buffer for the validity (nulls) bitmap
    # RecordBatch
    def Buffers(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 16
            from .Buffer import Buffer
            obj = Buffer()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # RecordBatch
    def BuffersLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

# /// Optional compression of the message body
    # RecordBatch
    def Compression(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from .BodyCompression import BodyCompression
            obj = BodyCompression()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def RecordBatchStart(builder): builder.StartObject(4)
def RecordBatchAddLength(builder, length): builder.PrependInt64Slot(0, length, 0)
def RecordBatchAddNodes(builder, nodes): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(nodes), 0)
def RecordBatchStartNodesVector(builder, numElems): return builder.StartVector(16, numElems, 8)
def RecordBatchAddBuffers(builder, buffers): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(buffers), 0)
def RecordBatchStartBuffersVector(builder, numElems): return builder.StartVector(16, numElems, 8)
def RecordBatchAddCompression(builder, compression): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(compression), 0)
def RecordBatchEnd(builder): return builder.EndObject()
