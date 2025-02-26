/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.external;

import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.FlexibleFieldSchema;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.external.AbstractElementDefinition;
import com.datasqrl.schema.input.external.FieldDefinition;
import com.datasqrl.schema.input.external.FieldTypeDefinitionImpl;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BasicType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SchemaExport {

  public TableDefinition export(FlexibleTableSchema table) {
    TableDefinition td = new TableDefinition();
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
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private Optional<FieldDefinition> export(FlexibleFieldSchema.Field field) {
    FieldDefinition fd = new FieldDefinition();
    exportElement(fd, field);
    List<FlexibleFieldSchema.FieldType> types = field.getTypes();
    if (types.isEmpty()) {
      return Optional.empty();
    } else if (types.size() == 1 && types.get(0).getVariantName().equals(SpecialName.SINGLETON)) {
      FieldTypeDefinitionImpl ftd = export(types.get(0));
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
    FieldTypeDefinitionImpl ftd = new FieldTypeDefinitionImpl();
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
