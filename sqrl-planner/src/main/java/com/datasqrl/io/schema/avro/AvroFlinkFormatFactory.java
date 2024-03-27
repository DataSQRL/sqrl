package com.datasqrl.io.schema.avro;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.FormatFactory;
import com.google.auto.service.AutoService;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;

@AutoService(FormatFactory.class)
@Getter
public class AvroFlinkFormatFactory extends FormatFactory.BaseFormatFactory {

  public static final String FORMAT_NAME = "avro";

  public static final Set<String> extensions = Set.of();

  public AvroFlinkFormatFactory() {
    super(FORMAT_NAME, extensions);
  }

  @Override
  public Format fromConfig(SqrlConfig connectorConfig) {
    return new AvroFormat();
  }

  @Override
  public Format createDefault() {
    return new AvroFormat();
  }

  public static class AvroFormat extends Format.BaseFormat {

    public AvroFormat() {
      super(FORMAT_NAME);
    }

    @Override
    public Optional<String> getSchemaType() {
      return Optional.of("avro");
    }

  }
}
