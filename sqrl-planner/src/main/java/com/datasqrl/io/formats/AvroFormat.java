package com.datasqrl.io.formats;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.NotYetImplementedException;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;

@AutoService(FormatFactoryOld.class)
public class AvroFormat implements FormatFactoryOld {

  public static final String FORMAT_NAME = "avro";
  public static final List<String> FORMAT_EXTENSIONS = List.of("avro");

  @Override
  public List<String> getExtensions() {
    return FORMAT_EXTENSIONS;
  }

  @Override
  public String getName() {
    return FORMAT_NAME;
  }

  @Override
  public Optional<String> getSchemaType() {
    return Optional.of("avro");
  }

  @Override
  public Parser getParser(@NonNull SqrlConfig config) {
    throw new NotYetImplementedException("Reading avro data directly not yet supported");
  }

  @Override
  public Writer getWriter(@NonNull SqrlConfig config) {
    throw new NotYetImplementedException("Writing avro data ");
  }
}
