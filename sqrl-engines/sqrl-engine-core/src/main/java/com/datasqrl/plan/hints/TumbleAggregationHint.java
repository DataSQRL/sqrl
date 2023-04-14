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
public class TumbleAggregationHint implements SqrlHint {

  public enum Type {FUNCTION, INSTANT}

  @Getter
  final int timestampIdx;
  @Getter
  final Type type;
  @Getter
  final int inputTimestampIdx;
  @Getter
  final long intervalMS;

  public static TumbleAggregationHint instantOf(int timestampIdx) {
    return new TumbleAggregationHint(timestampIdx, Type.INSTANT, -1, 1);
  }

  public static TumbleAggregationHint functionOf(int timestampIdx, int inputTimestampIdx,
      long intervalMS) {
    return new TumbleAggregationHint(timestampIdx, Type.FUNCTION, inputTimestampIdx, intervalMS);
  }

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(List.of(String.valueOf(timestampIdx), String.valueOf(type),
            String.valueOf(inputTimestampIdx), String.valueOf(intervalMS))).build();
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
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 4, "Invalid hint: %s", hint);
      return new TumbleAggregationHint(Integer.valueOf(options.get(0)),
          Type.valueOf(options.get(1)),
          Integer.valueOf(options.get(2)), Long.valueOf(options.get(3)));
    }
  }

}
