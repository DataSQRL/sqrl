package com.datasqrl.io.schema.json;

import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TableConfig.Format;
import com.datasqrl.config.FormatFactory;
import com.datasqrl.json.FlinkJsonType;
import com.google.auto.service.AutoService;
import java.util.List;
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

  public static List<Class> getSupportedTypeClasses() {
    return List.of(FlinkJsonType.class);
  }

  @Override
  public Format fromConfig(EngineConfig connectorConfig) {
    return new FlexibleJsonFormat();
  }

  @Override
  public Format createDefault() {
    return new FlexibleJsonFormat();
  }

  public static class FlexibleJsonFormat extends Format.BaseFormat {

    public FlexibleJsonFormat() {
      super(FORMAT_NAME);
    }

    @Override
    public Optional<String> getSchemaType() {
      return Optional.of("flexible");
    }

  }
}
