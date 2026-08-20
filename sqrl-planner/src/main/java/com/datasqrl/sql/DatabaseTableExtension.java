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
package com.datasqrl.sql;

import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import java.util.Collection;
import java.util.Optional;

/**
 * Service-loader extension point for database-specific DDL that depends on the full set of created
 * tables. Extensions may read their own settings from the engine configuration.
 */
public interface DatabaseTableExtension {

  String getName();

  String getDdl(
      Collection<CreateTableJdbcStatement> createTables, Optional<EngineConfig> engineConfig);

  default String getDdl(Collection<CreateTableJdbcStatement> createTables) {
    return getDdl(createTables, Optional.empty());
  }
}
