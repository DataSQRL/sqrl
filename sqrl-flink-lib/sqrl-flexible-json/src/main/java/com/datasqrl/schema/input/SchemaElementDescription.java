/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SchemaElementDescription implements Serializable {

  public static final SchemaElementDescription NONE = new SchemaElementDescription("");

  private String description;

  public boolean isEmpty() {
    return description == null || description.trim().isEmpty();
  }

  public static SchemaElementDescription of(String description) {
    if (description == null || description.trim().isEmpty()) {
      return NONE;
    } else {
      return new SchemaElementDescription(description);
    }
  }

}
