package ai.datasqrl.config;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.validate.imports.ImportManager;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class BundleOptions {
  ImportManager importManager;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;
}
