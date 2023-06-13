package com.datasqrl.kafka;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.Optional;

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
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    TableConfig tableConfig = new TableConfig(Name.system(ENGINE_NAME), config);
    Optional<String> schemaType = tableConfig.getSchemaType();
    Preconditions.checkArgument(schemaType.isPresent(), "Need to configure schema for log engine");
    TableSchemaExporterFactory schemaFactory = TableSchemaExporterFactory.load(schemaType.get());
    return new KafkaLogEngine(tableConfig, schemaFactory);
  }

}
