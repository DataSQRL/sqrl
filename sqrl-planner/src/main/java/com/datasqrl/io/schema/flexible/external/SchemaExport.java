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
package com.datasqrl.io.schema.flexible.external;

import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.input.external.AbstractElementDefinition;
import com.datasqrl.io.schema.flexible.input.external.FieldDefinition;
import com.datasqrl.io.schema.flexible.input.external.FieldTypeDefinitionImpl;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SchemaExport {

  public TableDefinition export(FlexibleTableSchema table) {
    var td = new TableDefinition();
    exportElement(td, table);
    td.tests = exportConstraints(table.getConstraints());
    td.partial_schema = table.isPartialSchema();
    td.columns = exportColumns(table.getFields());
    td.schema_version = SchemaImport.VERSION.toString();
    return td;
  }

  private void exportElement(AbstractElementDefinition element, FlexibleFieldSchema field) {
    element.name = field.getName().getDisplay();
    if (!field.getDescription().isEmpty()) {
      element.description = field.getDescription().getDescription();
    }
    element.default_value = field.getDefault_value();
  }

  private List<String> exportConstraints(List<Constraint> constraints) {
    if (constraints.isEmpty()) {
      return null;
    }
    // TODO: export parameters
    return constraints.stream().map(c -> c.getName().getDisplay()).collect(Collectors.toList());
  }

  private List<FieldDefinition> exportColumns(RelationType<FlexibleFieldSchema.Field> relation) {
    return relation.getFields().stream()
        .map(this::export)
        .flatMap(Optional::stream)
        .collect(Collectors.toList());
  }

  private Optional<FieldDefinition> export(FlexibleFieldSchema.Field field) {
    var fd = new FieldDefinition();
    exportElement(fd, field);
    List<FlexibleFieldSchema.FieldType> types = field.getTypes();
    if (types.isEmpty()) {
      return Optional.empty();
    } else if (types.size() == 1 && types.get(0).getVariantName().equals(SpecialName.SINGLETON)) {
      var ftd = export(types.get(0));
      fd.type = ftd.type;
      fd.columns = ftd.columns;
      fd.tests = ftd.tests;
    } else {
      fd.mixed =
          types.stream()
              .collect(Collectors.toMap(ft -> ft.getVariantName().getDisplay(), ft -> export(ft)));
    }
    return Optional.of(fd);
  }

  private FieldTypeDefinitionImpl export(FlexibleFieldSchema.FieldType fieldType) {
    var ftd = new FieldTypeDefinitionImpl();
    Type type = fieldType.getType();
    if (type instanceof BasicType) {
      ftd.type = SchemaImport.BasicTypeParse.export(fieldType);
    } else {
      Preconditions.checkArgument(type instanceof RelationType);
      ftd.columns = exportColumns((RelationType<FlexibleFieldSchema.Field>) type);
    }
    ftd.tests = exportConstraints(fieldType.getConstraints());
    return ftd;
  }
}
