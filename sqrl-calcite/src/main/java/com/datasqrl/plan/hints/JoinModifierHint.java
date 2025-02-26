/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.JoinModifier;

@Getter
@AllArgsConstructor
public class JoinModifierHint implements SqrlHint {

  final JoinModifier joinModifier;

  @Override
  public RelHint getHint() {
    return RelHint.builder(HINT_NAME).hintOption(joinModifier.name()).build();
  }

  public static final String HINT_NAME = JoinModifierHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<JoinModifierHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public JoinModifierHint fromHint(RelHint hint) {
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 1, "Invalid hint: %s", hint);
      JoinModifier modifier = JoinModifier.valueOf(options.get(0));
      return new JoinModifierHint(modifier);
    }
  }
}
