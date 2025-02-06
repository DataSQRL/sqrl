/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rel.hint.RelHint;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TimestampHint implements SqrlHint {

  protected final int timestampIdx;

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName()).hintOptions(
        IntStream.of(timestampIdx)
            .mapToObj(String::valueOf).collect(Collectors.toList())
    ).build();
  }

  public static final String HINT_NAME = TimestampHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<TimestampHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public TimestampHint fromHint(RelHint hint) {
      var options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 1, "Invalid hint: %s", hint);
      int timestampIdx = Integer.valueOf(options.getFirst());
      return new TimestampHint(timestampIdx);
    }
  }

}
