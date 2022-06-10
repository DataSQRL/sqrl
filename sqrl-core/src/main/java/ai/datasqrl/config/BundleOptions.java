package ai.datasqrl.config;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class BundleOptions {

  ImportManager importManager;
  SchemaAdjustmentSettings schemaSettings;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;


}
