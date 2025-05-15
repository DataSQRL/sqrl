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
package com.datasqrl.io.schema.flexible;

import com.datasqrl.io.schema.flexible.external.SchemaExport;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Strings;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

@Value
@AllArgsConstructor
public class FlexibleTableSchemaHolder implements TableSchema {

  @NonNull FlexibleTableSchema schema;
  String definition;
  @NonNull Optional<Path> location;

  public FlexibleTableSchemaHolder(FlexibleTableSchema schema) {
    this(schema, null, Optional.empty());
  }

  @Override
  public String getSchemaType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  @SneakyThrows
  public String getDefinition() {
    if (Strings.isNullOrEmpty(definition)) {
      SchemaExport schemaExport = new SchemaExport();
      TableDefinition schemaDef = schemaExport.export(schema);
      return Deserializer.INSTANCE.writeYML(schemaDef);
    }
    return definition;
  }
}
