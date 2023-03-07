/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

@AllArgsConstructor
@Getter
public class DedupHint implements SqrlHint {

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .build();
  }

  public static DedupHint of() {
    return new DedupHint();
  }

  public static final String HINT_NAME = DedupHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<DedupHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public DedupHint fromHint(RelHint hint) {
      List<String> opts = hint.listOptions;
      Preconditions.checkArgument(opts.size() == 0, "Invalid hint: %s", hint);
      return new DedupHint();
    }
  }

}
