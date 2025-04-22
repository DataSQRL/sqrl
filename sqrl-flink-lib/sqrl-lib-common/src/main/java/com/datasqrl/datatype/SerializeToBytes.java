package com.datasqrl.datatype;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

import lombok.SneakyThrows;

/**
 * Converts an annotated data type to
 */
@AutoService(AutoRegisterSystemFunction.class)
public class SerializeToBytes extends ScalarFunction implements AutoRegisterSystemFunction {

  @SneakyThrows
  public byte[] eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object object) {
    DataTypeHint hint = object.getClass().getAnnotation(DataTypeHint.class);
    Class<? extends TypeSerializer> serializerClass = hint.rawSerializer();

    TypeSerializer serializer = serializerClass.newInstance();

    DataOutputSerializer dos = new DataOutputSerializer(128);

    serializer.serialize(object, dos);

    return dos.getCopyOfBuffer();
  }
}
