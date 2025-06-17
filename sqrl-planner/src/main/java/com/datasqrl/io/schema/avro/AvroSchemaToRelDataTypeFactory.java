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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;

@AutoService(SchemaToRelDataTypeFactory.class)
@Slf4j
public class AvroSchemaToRelDataTypeFactory implements SchemaToRelDataTypeFactory {

  @Override
  public String getSchemaType() {
    return AvroTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public RelDataType map(TableSchema schema, Name tableName, ErrorCollector errors) {
    Preconditions.checkArgument(schema instanceof AvroSchemaHolder);
    var avroSchema = ((AvroSchemaHolder) schema).getSchema();

    var legacyTimestampMapping = getLegacyTimestampMapping();

    var converter = new AvroToRelDataTypeConverter(errors, legacyTimestampMapping);
    return converter.convert(avroSchema);
  }

  private boolean getLegacyTimestampMapping() {
    return false;
  }
}
