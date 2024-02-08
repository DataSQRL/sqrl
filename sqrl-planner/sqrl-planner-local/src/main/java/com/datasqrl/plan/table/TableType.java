/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum TableType {
  STREAM, //a stream of records with synthetic primary key ordered by timestamp
  VERSIONED_STATE, //table with natural primary key that ensures uniqueness and timestamp for change-stream
  STATE, //table with natural primary key that ensures uniqueness but not a versioned change-stream
  NESTED, //table that represents nested data to be joined with parent
  LOOKUP, //table that allows lookup by primary key but no other form of processing
  RELATION; //Relational data without timestamp, primary key or explicit stream-state semantics

  public boolean hasTimestamp() {
    switch(this) {
      case STREAM:
      case VERSIONED_STATE:
      case STATE:
        return true;
      default:
        return false; //NESTED, LOOKUP, RELATION
    }
  }

  public boolean hasPrimaryKey() {
    switch(this) {
      case STREAM:
      case VERSIONED_STATE:
      case STATE:
      case LOOKUP:
        return true;
      default:
        return false; //NESTED and RELATION
    }
  }

  public boolean validEndType() {
    switch(this) {
      case STREAM:
      case VERSIONED_STATE:
      case STATE:
      case RELATION:
        return true;
      default:
        return false; //NESTED and LOOKUP
    }
  }

  public boolean isStream() {
    return this==STREAM;
  }

  public boolean isState() {
    return this==VERSIONED_STATE || this==STATE;
  }

}
