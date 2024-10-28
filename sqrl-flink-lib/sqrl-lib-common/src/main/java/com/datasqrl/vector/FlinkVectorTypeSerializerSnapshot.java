package com.datasqrl.vector;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class FlinkVectorTypeSerializerSnapshot implements TypeSerializerSnapshot<FlinkVectorType> {

  private Class<FlinkVectorTypeSerializer> serializerClass;

  public FlinkVectorTypeSerializerSnapshot() {
    this.serializerClass = FlinkVectorTypeSerializer.class;
  }

  @Override
  public int getCurrentVersion() {
    return 1;
  }

  @Override
  public void writeSnapshot(DataOutputView out) throws IOException {
    out.writeUTF(FlinkVectorTypeSerializer.class.getName());
  }

  @Override
  public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
      throws IOException {
    String className = in.readUTF();
    try {
      this.serializerClass = (Class<FlinkVectorTypeSerializer>) Class.forName(className, true,
          userCodeClassLoader);
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to find serializer class: " + className, e);
    }
  }

  @Override
  public TypeSerializer restoreSerializer() {
    try {
      return serializerClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(
          "Failed to instantiate serializer class: " + serializerClass.getName(), e);
    }
  }

  @Override
  public TypeSerializerSchemaCompatibility resolveSchemaCompatibility(
      TypeSerializer newSerializer) {
    if (newSerializer.getClass() == this.serializerClass) {
      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    } else {
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}