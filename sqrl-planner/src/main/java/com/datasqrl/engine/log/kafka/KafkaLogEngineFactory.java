package com.datasqrl.engine.log.kafka;

import com.datasqrl.config.TableConfig.Format;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.google.auto.service.AutoService;
import java.util.Optional;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class KafkaLogEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "kafka";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Type getEngineType() {
    return Type.LOG;
  }

  @Override
  public KafkaLogEngine initialize(@NonNull EngineConfig connectorConfig,
      ConnectorFactory connectorFactory) {
    //This is hard-coded for now since Flink is the only engine we support
    Format format = connectorFactory.getFormat().get();
    Optional<TableSchemaExporterFactory> schemaExporterFactoryOpt;
    try {
      TableSchemaExporterFactory schemaFactory = TableSchemaExporterFactory.load(
          format.getSchemaType().get());
      schemaExporterFactoryOpt = Optional.of(schemaFactory);
    } catch (Exception e) {
      schemaExporterFactoryOpt = Optional.empty();
    }

    return new KafkaLogEngine(connectorConfig,
        schemaExporterFactoryOpt,
        connectorFactory);
  }
}
