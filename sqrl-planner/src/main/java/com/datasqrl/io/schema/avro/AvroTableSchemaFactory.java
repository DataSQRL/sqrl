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
package com.datasqrl.io.schema.avro;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.avro.Schema;

@AutoService(TableSchemaFactory.class)
public class AvroTableSchemaFactory implements TableSchemaFactory {

  public static final String SCHEMA_EXTENSION = ".avsc";

  public static final String SCHEMA_TYPE = "avro";

  @Override
  public AvroSchemaHolder create(
      String schemaDefinition, Optional<Path> location, ErrorCollector errors) {
    if (location.isPresent()) {
      errors = errors.withConfig(location.get());
    }
    Schema schema;
    try {
      schema = new Schema.Parser().parse(schemaDefinition);
    } catch (Exception e) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Could not parse schema: %s", e);
    }
    return new AvroSchemaHolder(schema, schemaDefinition, location);
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }

  @Override
  public String getExtension() {
    return SCHEMA_EXTENSION;
  }
}
