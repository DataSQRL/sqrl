package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.List;

public class NoQueryHint extends ColumnNamesHint implements QueryByHint  {

  public static final String HINT_NAME = "no_query";

  protected NoQueryHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER, List.of());
  }

  @AutoService(Factory.class)
  public static class NoQueryHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      Preconditions.checkArgument(source.get().getOptions().isEmpty(), "no_query hint does not accept options");
      return new NoQueryHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
