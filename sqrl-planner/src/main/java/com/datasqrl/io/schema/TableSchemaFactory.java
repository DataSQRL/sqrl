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
package com.datasqrl.io.schema;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public interface TableSchemaFactory {

  SchemaConversionResult convert(Path location, ErrorCollector errors);

  String getType();

  Set<String> getExtensions();

  static Map<String, TableSchemaFactory> factoriesByExtension() {
    return ServiceLoaderDiscovery.getAll(TableSchemaFactory.class).stream()
        .flatMap(tsf -> tsf.getExtensions().stream().map(ext -> Pair.of(tsf, ext)))
        .collect(Collectors.toMap(Pair::getRight, Pair::getLeft));
  }
}
