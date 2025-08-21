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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.converters.FlexibleTable2RelDataTypeConverter;
import com.datasqrl.io.schema.flexible.external.SchemaImport;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.BaseFileUtil;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {

  private static final Set<String> SCHEMA_EXTENSION = Set.of("schema.yml");
  private static final String SCHEMA_TYPE = "flexible";

  @Override
  public SchemaConversionResult convert(Path location, ErrorCollector errors) {
    var schemaDefinition = BaseFileUtil.readFile(location);
    errors = errors.withScript(location, schemaDefinition);
    var deserializer = Deserializer.INSTANCE;
    var schemaDef = deserializer.mapYAML(schemaDefinition, TableDefinition.class);
    var importer = new SchemaImport(Constraint.FACTORY_LOOKUP, NameCanonicalizer.SYSTEM);
    var tableSchema = importer.convert(schemaDef, errors).get();
    return new SchemaConversionResult(
        convert(tableSchema, location.getFileName().toString()), Map.of());
  }

  public static RelDataType convert(FlexibleTableSchema tableSchema, String tableName) {
    var converter = new FlexibleTableConverter(tableSchema, Name.system(tableName));
    var relDataTypeConverter = new FlexibleTable2RelDataTypeConverter();
    return converter.apply(relDataTypeConverter);
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }

  @Override
  public Set<String> getExtensions() {
    return SCHEMA_EXTENSION;
  }
}
