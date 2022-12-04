/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

public enum TableType {
  STREAM, //a stream of records with synthetic (i.e. uuid) primary key ordered by timestamp
  TEMPORAL_STATE, //table with natural primary key that ensures uniqueness and timestamp for
  // change-stream
  STATE; //table with natural primary key that ensures uniqueness but no timestamp (i.e.
  // represents timeless state)

  public boolean hasTimestamp() {
    return this != STATE;
  }
}
