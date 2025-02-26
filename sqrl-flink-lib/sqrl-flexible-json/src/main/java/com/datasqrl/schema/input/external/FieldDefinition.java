/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input.external;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
public class FieldDefinition extends AbstractElementDefinition implements FieldTypeDefinition {

  public String type;
  public List<FieldDefinition> columns;
  public List<String> tests;
  public Map<String, Object> cardinality;

  public Map<String, FieldTypeDefinitionImpl> mixed;
}
