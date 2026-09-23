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
package com.datasqrl.discovery.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface RecordReader {

  String getFormat();

  Stream<Map<String, Object>> read(InputStream input) throws IOException;

  Set<String> getExtensions();

  /**
   * Returns format-specific options, without the format identifier prefix.
   *
   * <p>For example, an option returned as {@code timestamp-format.standard} for a format named
   * {@code flexible-json} is emitted as {@code flexible-json.timestamp-format.standard} in the
   * Flink table definition.
   */
  default Map<String, String> getFormatOptions() {
    return Map.of();
  }
}
