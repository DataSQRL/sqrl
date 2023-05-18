/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input.external;

import java.io.Serializable;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class AbstractElementDefinition implements Serializable {

  public String name;
  public String description;

  public Object default_value;
  //TODO: add hints


  public AbstractElementDefinition(String name, String description, Object default_value) {
    this.name = name;
    this.description = description;
    this.default_value = default_value;
  }
}
