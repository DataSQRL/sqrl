/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input.external;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class FieldDefinition extends AbstractElementDefinition implements FieldTypeDefinition {

  public String type;
  public List<FieldDefinition> columns;
  public List<String> tests;

  public Map<String, FieldTypeDefinitionImpl> mixed;

  public FieldDefinition(String name, String description, Object default_value, String type,
      List<FieldDefinition> columns, List<String> tests,
      Map<String, FieldTypeDefinitionImpl> mixed) {
    super(name, description, default_value);
    this.type = type;
    this.columns = columns;
    this.tests = tests;
    this.mixed = mixed;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public List<FieldDefinition> getColumns() {
    return columns;
  }

  @Override
  public List<String> getTests() {
    return tests;
  }
}
