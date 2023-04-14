/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@AllArgsConstructor
public class TemporalJoinHint implements SqrlHint {


  final int streamTimestampIdx;
  final int stateTimestampIdx;
  final int[] statePrimaryKeys;

  @Override
  public RelHint getHint() {
    return RelHint.builder(HINT_NAME).hintOptions(
        IntStream.concat(IntStream.of(streamTimestampIdx, stateTimestampIdx),
                IntStream.of(statePrimaryKeys))
            .mapToObj(String::valueOf).collect(Collectors.toList())
    ).build();
  }

  public static final String HINT_NAME = TemporalJoinHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<TemporalJoinHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public TemporalJoinHint fromHint(RelHint hint) {
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(options.size() >= 2, "Invalid hint: %s", hint);
      int streamTimeIdx = Integer.valueOf(options.get(0));
      int stateTimeIdx = Integer.valueOf(options.get(1));
      int[] pks = new int[options.size() - 2];
      for (int i = 2; i < options.size(); i++) {
        pks[i - 2] = Integer.valueOf(options.get(i));
      }
      return new TemporalJoinHint(streamTimeIdx, stateTimeIdx, pks);
    }
  }

}
