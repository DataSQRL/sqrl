/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

@AllArgsConstructor
@Getter
public class SlidingAggregationHint implements SqrlHint {

  final int timestampIdx;

  final long intervalWidthMs;
  final long slideWidthMs;

  @Override
  public RelHint getHint() {
    return RelHint.builder(HINT_NAME).hintOptions(List.of(String.valueOf(timestampIdx),
        String.valueOf(intervalWidthMs), String.valueOf(slideWidthMs))).build();
  }

  public static final String HINT_NAME = SlidingAggregationHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<SlidingAggregationHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public SlidingAggregationHint fromHint(RelHint hint) {
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(hint.listOptions.size() == 3, "Invalid hint: %s", hint);
      return new SlidingAggregationHint(Integer.valueOf(options.get(0)),
          Long.valueOf(options.get(1)), Long.valueOf(options.get(2)));
    }
  }

}
