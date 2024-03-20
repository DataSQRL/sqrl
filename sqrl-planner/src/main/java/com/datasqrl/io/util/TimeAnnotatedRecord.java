/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.util;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class TimeAnnotatedRecord<R> implements Serializable {

  @NonNull R record;
  Instant sourceTime;

  public TimeAnnotatedRecord(R record) {
    this(record, null);
  }

  public boolean hasTime() {
    return sourceTime != null;
  }

  @Override
  public String toString() {
    String result;
    if (record instanceof String) {
      result = "\"" + record + "\"";
    } else {
      result = String.valueOf(record);
    }
    if (hasTime()) result+= "@"+String.valueOf(sourceTime);
    return result;
  }

}
