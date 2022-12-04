/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.util;

import lombok.*;

import java.io.Serializable;
import java.time.Instant;

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

}
