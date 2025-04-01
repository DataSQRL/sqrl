package com.datasqrl.datatype;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts an annotated data type to
 */
@AutoService(StandardLibraryFunction.class)
public class SerializeToBytes extends ScalarFunction implements SqrlCastFunction, StandardLibraryFunction {

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
