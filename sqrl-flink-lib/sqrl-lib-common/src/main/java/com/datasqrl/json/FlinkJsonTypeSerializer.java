package com.datasqrl.json;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FlinkJsonTypeSerializer extends TypeSerializer<FlinkJsonType> {

    ObjectMapper mapper = new ObjectMapper();
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
        byte[] jsonData = mapper.writeValueAsBytes(record.getJson());
        target.writeInt(jsonData.length);
        target.write(jsonData);
    }

    @Override
    public FlinkJsonType deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] jsonData = new byte[length];
        source.readFully(jsonData);
        return new FlinkJsonType(mapper.readTree(jsonData));
    }

    @Override
    public FlinkJsonType deserialize(FlinkJsonType reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        byte[] jsonData = new byte[length];
        source.readFully(jsonData);
        target.writeInt(length);
        target.write(jsonData);
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
