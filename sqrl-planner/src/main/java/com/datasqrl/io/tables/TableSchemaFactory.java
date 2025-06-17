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
package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.nio.file.Path;
import java.util.Optional;

public interface TableSchemaFactory {

  TableSchema create(String schemaDefinition, Optional<Path> location, ErrorCollector errors);

  String getType();

  String getExtension();

  static TableSchemaFactory loadByType(String schemaType) {
    return ServiceLoaderDiscovery.get(
        TableSchemaFactory.class, TableSchemaFactory::getType, schemaType);
  }

  static Optional<TableSchemaFactory> loadByExtension(String extension) {
    return ServiceLoaderDiscovery.findFirst(
        TableSchemaFactory.class, TableSchemaFactory::getExtension, extension);
  }
}
