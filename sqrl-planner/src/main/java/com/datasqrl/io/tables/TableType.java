/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.io.tables;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum TableType {
  STREAM, // a stream of records with synthetic primary key ordered by timestamp
  VERSIONED_STATE, // table with natural primary key that ensures uniqueness and timestamp for
  // change-stream
  STATE, // table with natural primary key that ensures uniqueness but not a versioned change-stream
  LOOKUP, // table that allows lookup by primary key but no other form of processing
  // =add to temporal join
  RELATION, // the default state if we cannot infer it
  STATIC; // A set of data that does not change over time and valid for all time (e.g. values, table

  // functions, or nested data)

  // =use timestamp of other side in join, update generic aggregate logic, write to database without
  // timestamp

  public boolean hasTimestamp() {
    return switch (this) {
      case STREAM, VERSIONED_STATE, STATE -> true;
      default -> false; // NESTED, LOOKUP, RELATION
    };
  }

  public boolean isLocked() {
    return this == LOOKUP;
  }

  public boolean hasPrimaryKey() {
    return switch (this) {
      case STREAM, VERSIONED_STATE, STATE, LOOKUP, STATIC -> true;
      default -> false; // NESTED and RELATION
    };
  }

  public TableType combine(TableType other) {
    if (this == LOOKUP || other == LOOKUP) {
      return RELATION; // or throw exception?
    }
    if (this == STATIC) {
      return other;
    }
    if (other == STATIC) {
      return this;
    }
    if (this == RELATION || other == RELATION) {
      return RELATION;
    }
    if (this == STREAM && other == STREAM) {
      return STREAM;
    }
    return STATE;
  }

  public boolean supportsTemporalJoin() {
    return this == VERSIONED_STATE || this == LOOKUP;
  }

  public boolean isStream() {
    return this == STREAM;
  }

  public boolean isState() {
    return this == VERSIONED_STATE || this == STATE;
  }
}
