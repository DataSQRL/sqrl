package com.datasqrl.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

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
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    TableConfig tableConfig = new TableConfig(Name.system(ENGINE_NAME), config);
    String schemaType = tableConfig.hasFormat()?tableConfig.getFormat().getName():tableConfig.getBase().getSchema();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaType), "Need to configure schema for log engine");
    TableSchemaExporterFactory schemaFactory = TableSchemaExporterFactory.load(schemaType);
    return new KafkaLogEngine(tableConfig, schemaFactory);
  }
}
