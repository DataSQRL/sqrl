package com.datasqrl.vector;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import java.io.IOException;

public class FlinkVectorTypeSerializer extends TypeSerializer<FlinkVectorType> {

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public FlinkVectorType createInstance() {
    return new FlinkVectorType(null);
  }

  @Override
  public FlinkVectorType copy(FlinkVectorType from) {
    return new FlinkVectorType(from.getValue());
  }

  @Override
  public FlinkVectorType copy(FlinkVectorType from, FlinkVectorType reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1; // indicates that this serializer does not have a fixed length
  }

  @Override
  public void serialize(FlinkVectorType record, DataOutputView target) throws IOException {
    target.writeInt(record.getValue().length); // First write the length of the array
    for (double v : record.getValue()) {
      target.writeDouble(v); // Write each double value
    }
  }

  @Override
  public FlinkVectorType deserialize(DataInputView source) throws IOException {
    int length = source.readInt();
    double[] array = new double[length];
    for (int i = 0; i < length; i++) {
      array[i] = source.readDouble();
    }
    return new FlinkVectorType(array);
  }

  @Override
  public FlinkVectorType deserialize(FlinkVectorType reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeUTF(source.readUTF());
  }

  @Override
  public TypeSerializer<FlinkVectorType> duplicate() {
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FlinkVectorTypeSerializer;
  }

  @Override
  public int hashCode() {
    return FlinkVectorTypeSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<FlinkVectorType> snapshotConfiguration() {
    return new FlinkVectorTypeSerializerSnapshot();
  }

}
