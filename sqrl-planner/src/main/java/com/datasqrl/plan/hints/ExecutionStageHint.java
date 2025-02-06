/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

@Getter
@AllArgsConstructor
public class ExecutionStageHint implements SqrlHint {

  final String stageName;

  @Override
  public RelHint getHint() {
    return RelHint.builder(HINT_NAME).hintOptions(List.of(stageName)).build();
  }

  public static final String HINT_NAME = ExecutionStageHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<ExecutionStageHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public ExecutionStageHint fromHint(RelHint hint) {
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 1, "Invalid hint: %s", hint);
      return new ExecutionStageHint(options.getFirst());
    }
  }

}
