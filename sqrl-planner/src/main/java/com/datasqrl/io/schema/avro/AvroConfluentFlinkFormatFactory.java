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
public class AvroConfluentFlinkFormatFactory extends FormatFactory.BaseFormatFactory {

  public static final String FORMAT_NAME = "avro-confluent";

  public static final Set<String> extensions = Set.of();

  public AvroConfluentFlinkFormatFactory() {
    super(FORMAT_NAME, extensions);
  }

  @Override
  public Format fromConfig(SqrlConfig connectorConfig) {
    return new AvroConfluentFormat();
  }

  @Override
  public Format createDefault() {
    return new AvroConfluentFormat();
  }

  public static class AvroConfluentFormat extends Format.BaseFormat {

    public AvroConfluentFormat() {
      super(FORMAT_NAME);
    }

    @Override
    public Optional<String> getSchemaType() {
      return Optional.of("avro");
    }

  }
}

