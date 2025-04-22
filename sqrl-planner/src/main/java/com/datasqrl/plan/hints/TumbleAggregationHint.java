/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import java.util.List;

import org.apache.calcite.rel.hint.RelHint;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class TumbleAggregationHint implements SqrlHint {

  public enum Type {FUNCTION, INSTANT}

  @Getter
  final int windowFunctionIdx;
  @Getter
  final Type type;
  @Getter
  final int inputTimestampIdx;
  @Getter
  final long windowWidthMs;
  @Getter
  final long windowOffsetMs;

  public static TumbleAggregationHint instantOf(int timestampIdx) {
    return new TumbleAggregationHint(timestampIdx, Type.INSTANT, timestampIdx, 1, 0);
  }

  public static TumbleAggregationHint functionOf(int windowFunctionIdx, int inputTimestampIdx,
      long windowWidthMs, long windowOffsetMs) {
    return new TumbleAggregationHint(windowFunctionIdx, Type.FUNCTION, inputTimestampIdx, windowWidthMs, windowOffsetMs);
  }

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(List.of(String.valueOf(windowFunctionIdx), String.valueOf(type),
            String.valueOf(inputTimestampIdx), String.valueOf(windowWidthMs),
            String.valueOf(windowOffsetMs))).build();
  }

  public static final String HINT_NAME = TumbleAggregationHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<TumbleAggregationHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public TumbleAggregationHint fromHint(RelHint hint) {
      var options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 5, "Invalid hint: %s", hint);
      return new TumbleAggregationHint(Integer.valueOf(options.get(0)),
          Type.valueOf(options.get(1)),
          Integer.valueOf(options.get(2)), Long.valueOf(options.get(3)), Long.valueOf(options.get(4)));
    }
  }

}
