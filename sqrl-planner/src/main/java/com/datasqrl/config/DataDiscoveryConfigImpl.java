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
package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.Value;

@Value
public class DataDiscoveryConfigImpl implements PackageJson.DataDiscoveryConfig {

  public static final String TABLE_PATTERN_KEY = "pattern";

  ErrorCollector errors;
  Optional<String> tablePattern;

  public static DataDiscoveryConfigImpl of(SqrlConfig config, @NonNull ErrorCollector errors) {
    Optional<Pattern> pattern = Optional.empty();
    var tablePattern =
        config
            .asString(TABLE_PATTERN_KEY)
            .validate(TablePattern::isValid, "Not a valid regular expression for the table pattern")
            .getOptional();
    return new DataDiscoveryConfigImpl(errors, tablePattern);
  }

  @Override
  public TablePattern getTablePattern(String defaultPattern) {
    return TablePattern.of(tablePattern, defaultPattern);
  }
}
