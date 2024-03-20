package com.datasqrl.io.formats;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;

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

  public static class FlexibleJsonFormat extends Format.BaseFormat implements StringSerializableFormat {

    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

    public FlexibleJsonFormat() {
      super(FORMAT_NAME);
    }

    @Override
    public Optional<String> getSchemaType() {
      return Optional.of("flexible");
    }

    @Override
    public String serialize(Map<String, Object> data, Optional<TableSchema> schema) throws Exception {
      return mapper.writeValueAsString(data);
    }

    @Override
    public Map<String, Object> deserialize(String data, Optional<TableSchema> schema) throws Exception {
      return mapper.readValue(data, LinkedHashMap.class);
    }
  }
}
