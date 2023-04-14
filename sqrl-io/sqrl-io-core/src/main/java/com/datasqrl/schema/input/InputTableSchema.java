/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.io.tables.TableSchema;
import java.io.Serializable;
import lombok.Value;

@Value
public class InputTableSchema<T extends TableSchema> implements Serializable {

  private final T schema;
  private final boolean hasSourceTimestamp;

}
