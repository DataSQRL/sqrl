/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.input;

import lombok.*;

import java.io.Serializable;

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
