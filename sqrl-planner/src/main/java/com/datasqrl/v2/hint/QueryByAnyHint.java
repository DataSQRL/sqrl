package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.google.auto.service.AutoService;

public class QueryByAnyHint extends ColumnNamesHint implements QueryByHint {

  public static final String HINT_NAME = "query_by_any";

  protected QueryByAnyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER, source.get().getOptions());
  }

  @AutoService(Factory.class)
  public static class QueryByAnyFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new QueryByAnyHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
