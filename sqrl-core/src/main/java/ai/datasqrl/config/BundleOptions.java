package ai.datasqrl.config;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
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
  JDBCConnectionProvider dbConnection;
  StreamEngine streamEngine;


}
