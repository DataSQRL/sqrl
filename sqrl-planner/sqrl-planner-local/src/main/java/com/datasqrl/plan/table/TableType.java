/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum TableType {
  STREAM(true), //a stream of records with synthetic (i.e. uuid) primary key ordered by timestamp
  DEDUP_STREAM(false), //table with natural primary key that ensures uniqueness and timestamp for
  // change-stream
  STATE(false); //table with natural primary key that ensures uniqueness but no timestamp (i.e.
  // represents timeless state)

  private final boolean isStream;

  public boolean hasTimestamp() {
    return true;
  }

  public boolean isStream() {
    return isStream;
  }

  public boolean isState() {
    return !isStream;
  }

}
