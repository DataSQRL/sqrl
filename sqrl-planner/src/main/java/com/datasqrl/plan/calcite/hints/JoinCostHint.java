package com.datasqrl.plan.calcite.hints;

import com.datasqrl.plan.calcite.table.TableType;
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
  final int numEqualities;

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(List.of(leftType.name(), rightType.name(), String.valueOf(numEqualities)))
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
      Preconditions.checkArgument(opts.size() == 3, "Invalid hint: %s", hint);
      return new JoinCostHint(TableType.valueOf(opts.get(0)), TableType.valueOf(opts.get(1)),
          Integer.valueOf(hint.listOptions.get(2)));
    }
  }

}
