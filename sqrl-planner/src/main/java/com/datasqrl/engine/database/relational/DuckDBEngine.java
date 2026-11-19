/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.server.jdbc.DatabaseType;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.NonNull;

public class DuckDBEngine extends AbstractJDBCQueryEngine {

  static final String S3_ENDPOINT_KEY = "s3-endpoint";
  static final String USE_CREDENTIAL_CHAIN_KEY = "use-credential-chain";

  @Inject
  public DuckDBEngine(
      @NonNull PackageJson json, ConnectorFactoryFactory connectorFactory, ErrorCollector errors) {
    super(
        DuckDBEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfigOrEmpty(DuckDBEngineFactory.ENGINE_NAME),
        connectorFactory);

    // The package schema checks each package file before the files are merged, so a rule over
    // two keys is checked here, on the merged config. The server checks it again at startup.
    var hasS3Endpoint =
        engineConfig.getPropertyOptional(S3_ENDPOINT_KEY).filter(e -> !e.isBlank()).isPresent();
    var useCredentialChain =
        engineConfig.getPropertyAsOptional(USE_CREDENTIAL_CHAIN_KEY, Boolean.class).orElse(false);
    errors.checkFatal(
        !hasS3Endpoint || useCredentialChain,
        "The DuckDB engine sets '%s' but not '%s': true. The endpoint only applies to the S3"
            + " secret that '%s' creates.",
        S3_ENDPOINT_KEY,
        USE_CREDENTIAL_CHAIN_KEY,
        USE_CREDENTIAL_CHAIN_KEY);
  }

  @Override
  public Optional<String> serverConfigName() {
    return Optional.of("duckDbConfig");
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected DatabaseType getDatabaseType() {
    return DatabaseType.DUCKDB;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new DuckDbStatementFactory(engineConfig);
  }
}
