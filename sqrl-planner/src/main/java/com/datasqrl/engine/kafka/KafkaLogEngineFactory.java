package com.datasqrl.engine.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.io.formats.Format;
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
  public KafkaLogEngine initialize(@NonNull SqrlConfig connectorConfig) {
    //This is hard-coded for now since Flink is the only engine we support
    KafkaConnectorFactory kafkaConnector = KafkaFlinkConnectorFactory.INSTANCE;
    Format format = kafkaConnector.getFormat(connectorConfig);
    Optional<TableSchemaExporterFactory> schemaExporterFactoryOpt;
    try {
      TableSchemaExporterFactory schemaFactory = TableSchemaExporterFactory.load(
          format.getSchemaType().get());
      schemaExporterFactoryOpt = Optional.of(schemaFactory);
    } catch (Exception e) {
      schemaExporterFactoryOpt = Optional.empty();
    }
    return new KafkaLogEngine(connectorConfig, schemaExporterFactoryOpt,
        kafkaConnector);
  }

}
