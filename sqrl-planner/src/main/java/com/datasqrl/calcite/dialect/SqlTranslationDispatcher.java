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
package com.datasqrl.calcite.dialect;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.translation.SqlTranslation;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class SqlTranslationDispatcher {

  private static final Map<Dialect, Map<String, SqlTranslation>> DIALECT_TRANSLATIONS =
      Map.copyOf(
          ServiceLoaderDiscovery.getAll(SqlTranslation.class).stream()
              .collect(
                  Collectors.groupingBy(
                      SqlTranslation::getDialect,
                      () -> new EnumMap<>(Dialect.class),
                      Collectors.collectingAndThen(
                          Collectors.toMap(
                              translation -> operatorKey(translation.getOperator().getName()),
                              translation -> translation),
                          Map::copyOf))));

  static boolean tryUnparseTranslatedCall(
      Dialect dialect, SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {

    var dialectTranslations = DIALECT_TRANSLATIONS.getOrDefault(dialect, Map.of());
    var translation = dialectTranslations.get(operatorKey(call.getOperator().getName()));
    if (translation == null) {
      return false;
    }

    translation.unparse(call, writer, leftPrec, rightPrec);

    return true;
  }

  private static String operatorKey(String operatorName) {
    return operatorName.toLowerCase();
  }
}
