package com.datasqrl.io.schema.json;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import lombok.Getter;

import java.util.Optional;
import java.util.Set;

@AutoService(FormatFactory.class)
@Getter
public class FlexibleJsonFlinkFormatFactory extends FormatFactory.BaseFormatFactory {

  public static final String FORMAT_NAME = "flexible-json";

  public static final Set<String> extensions = Set.of("json");

  public FlexibleJsonFlinkFormatFactory() {
    super(FORMAT_NAME, extensions);
  }

  @Override
  public Format fromConfig(SqrlConfig connectorConfig) {
    return new FlexibleJsonFormat();
  }

  @Override
  public Format createDefault() {
    return new FlexibleJsonFormat();
  }

  public static class FlexibleJsonFormat extends Format.BaseFormat {

    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

    public FlexibleJsonFormat() {
      super(FORMAT_NAME);
    }

    @Override
    public Optional<String> getSchemaType() {
      return Optional.of("flexible");
    }

  }
}
