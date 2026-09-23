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
package com.datasqrl.server.jdbc;

import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import io.vertx.core.json.JsonArray;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class VertxParamArgumentTypeMapper implements ParamArgumentTypeMapper {

  private final Map<DatabaseType, VertxDatabaseParameterConverter> databaseConverters;
  private final VertxDatabaseParameterConverter defaultConverter;

  public VertxParamArgumentTypeMapper() {
    this(new DefaultVertxDatabaseParameterConverter());
  }

  private VertxParamArgumentTypeMapper(VertxDatabaseParameterConverter defaultConverter) {
    this(
        Map.of(
            DatabaseType.POSTGRES, new PostgresVertxDatabaseParameterConverter(defaultConverter)),
        defaultConverter);
  }

  public VertxParamArgumentTypeMapper(
      Map<DatabaseType, VertxDatabaseParameterConverter> databaseConverters,
      VertxDatabaseParameterConverter defaultConverter) {
    this.databaseConverters = Map.copyOf(databaseConverters);
    this.defaultConverter = defaultConverter;
  }

  @Override
  public Object map(Object param, Optional<String> sqlType, DatabaseType databaseType) {
    if (param instanceof List<?> l) {
      return l.toArray();
    }

    if (param instanceof JsonArray arr) {
      // Unwrap JsonArray to plain Java array to avoid pgclient treating it as JSONB
      return arr.getList().toArray();
    }

    if (param instanceof FlinkVectorType vec) {
      return Arrays.toString(vec.getValue());
    }

    return databaseConverters.getOrDefault(databaseType, defaultConverter).convert(param, sqlType);
  }
}
