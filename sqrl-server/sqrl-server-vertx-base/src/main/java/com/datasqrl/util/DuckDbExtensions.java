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
package com.datasqrl.util;

import java.util.Map;
import java.util.StringJoiner;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class DuckDbExtensions {

  private final StringJoiner joiner = new StringJoiner(";", "", ";");

  private final Map<String, Object> config;

  public String buildInitSql() {
    joiner.add("INSTALL iceberg");
    joiner.add("INSTALL httpfs");
    ifDiskCache("INSTALL cache_httpfs FROM community");

    joiner.add("LOAD iceberg");
    joiner.add("LOAD httpfs");
    ifDiskCache("LOAD cache_httpfs");

    if (asBoolean("use-version-guessing")) {
      joiner.add("SET unsafe_enable_version_guessing = true");
    }

    return joiner.toString();
  }

  private void ifDiskCache(String stmt) {
    if (asBoolean("use-disk-cache")) {
      joiner.add(stmt);
    }
  }

  private boolean asBoolean(String key) {
    return (boolean) config.getOrDefault(key, false);
  }
}
