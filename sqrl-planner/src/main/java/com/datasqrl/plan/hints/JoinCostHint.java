/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.datasqrl.plan.rules.JoinAnalysis;
import com.datasqrl.plan.rules.JoinAnalysis.Side;
import com.datasqrl.io.tables.TableType;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

@AllArgsConstructor
@Getter
public class JoinCostHint implements SqrlHint {

  final TableType leftType;
  final TableType rightType;
  /**
   * The number of equality constraints between the two sides of the join
   * to estimate cardinality of the result
   */
  final int numEqualities;
  /**
   * The side which has (at most) a single row matching any given row on the
   * other side because of a pk constraint.
   */
  final JoinAnalysis.Side singletonSide;

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(List.of(leftType.name(), rightType.name(), String.valueOf(numEqualities), String.valueOf(
            singletonSide)))
        .build();
  }

  public static final String HINT_NAME = JoinCostHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<JoinCostHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public JoinCostHint fromHint(RelHint hint) {
      List<String> opts = hint.listOptions;
      Preconditions.checkArgument(opts.size() == 4, "Invalid hint: %s", hint);
      return new JoinCostHint(TableType.valueOf(opts.get(0)), TableType.valueOf(opts.get(1)),
          Integer.valueOf(hint.listOptions.get(2)), Side.valueOf(hint.listOptions.get(3)));
    }
  }

}
