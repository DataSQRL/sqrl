/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum TableType {
  STREAM, //a stream of records with synthetic primary key ordered by timestamp
  VERSIONED_STATE, //table with natural primary key that ensures uniqueness and timestamp for change-stream
  STATE, //table with natural primary key that ensures uniqueness but not a versioned change-stream
  LOOKUP, //table that allows lookup by primary key but no other form of processing
  //=add to temporal join
  RELATION, //Relational data without timestamp, primary key or explicit stream-state semantics
  //=overload in generic visit(RelNode) in parent, inline select map, and select query stage
  STATIC; //A set of data that does not change over time and valid for all time (e.g. values, table functions, or nested data)
  //=use timestamp of other side in join, update generic aggregate logic, write to database without timestamp

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

  public boolean isLocked() {
    return this==LOOKUP;
  }

  public boolean hasPrimaryKey() {
    switch(this) {
      case STREAM:
      case VERSIONED_STATE:
      case STATE:
      case LOOKUP:
      case STATIC:
        return true;
      default:
        return false; //NESTED and RELATION
    }
  }

  public boolean isStream() {
    return this==STREAM;
  }

  public boolean isState() {
    return this==VERSIONED_STATE || this==STATE;
  }

}
