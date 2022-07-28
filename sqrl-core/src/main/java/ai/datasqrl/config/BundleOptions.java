package ai.datasqrl.config;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class BundleOptions {

  ImportManager importManager;
  SchemaAdjustmentSettings schemaSettings;
  JDBCConnectionProvider dbConnection;
  StreamEngine streamEngine;


}
