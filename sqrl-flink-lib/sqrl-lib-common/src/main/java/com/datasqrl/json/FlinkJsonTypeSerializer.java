package com.datasqrl.json;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import java.io.IOException;

public class FlinkJsonTypeSerializer extends TypeSerializer<FlinkJsonType> {

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public FlinkJsonType createInstance() {
        return new FlinkJsonType(null);
    }

    @Override
    public FlinkJsonType copy(FlinkJsonType from) {
        return new FlinkJsonType(from.getJson());
    }

    @Override
    public FlinkJsonType copy(FlinkJsonType from, FlinkJsonType reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // indicates that this serializer does not have a fixed length
    }

    @Override
    public void serialize(FlinkJsonType record, DataOutputView target) throws IOException {
        byte[] bytes = record.getJson().getBytes();
        target.writeInt(bytes.length); // First write the length of the array
        target.write(bytes);
    }

    @Override
    public FlinkJsonType deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] array = new byte[length];
        for (int i = 0; i < length; i++) {
            array[i] = source.readByte();
        }
        return new FlinkJsonType(new String(array));
    }

    @Override
    public FlinkJsonType deserialize(FlinkJsonType reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
    }

    @Override
    public TypeSerializer<FlinkJsonType> duplicate() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FlinkJsonTypeSerializer;
    }

    @Override
    public int hashCode() {
        return FlinkJsonTypeSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<FlinkJsonType> snapshotConfiguration() {
        return new FlinkJsonTypeSerializerSnapshot();
    }

}
