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

import io.vertx.sqlclient.data.NullValue;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/** Handles PostgreSQL's timestamptz bindings, which require {@link OffsetDateTime}. */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class PostgresVertxDatabaseParameterConverter implements VertxDatabaseParameterConverter {

  private final VertxDatabaseParameterConverter delegate;

  @Override
  public Object convert(Object param, Optional<String> sqlType) {
    if (param == null && sqlType.filter("TIMESTAMP"::equals).isPresent()) {
      return NullValue.of(OffsetDateTime.class);
    }

    var converted = delegate.convert(param, sqlType);
    if (converted instanceof LocalDateTime localDateTime) {
      return localDateTime.atOffset(ZoneOffset.UTC);
    }

    return converted;
  }
}
