package ai.datasqrl.config;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.plan.calcite.CalciteEnvironment;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class BundleOptions {

  ImportManager importManager;
  SchemaAdjustmentSettings schemaSettings;
  CalciteEnvironment calciteEnv;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;


}
