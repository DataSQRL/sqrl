/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import lombok.Value;

import java.io.Serializable;

@Value
public class InputTableSchema implements Serializable {

  private final FlexibleDatasetSchema.TableField schema;
  private final boolean hasSourceTimestamp;


}
