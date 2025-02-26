package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.google.auto.service.AutoService;

/**
 * Defines a table access function for the table that queries by all of the given columns
 * (i.e. an AND condition) and all columns must be set (i.e. no nulls accepted)
 */
public class QueryByAllHint extends ColumnNamesHint implements QueryByHint  {

  public static final String HINT_NAME = "query_by_all";

  protected QueryByAllHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER, source.get().getOptions());
  }

  @AutoService(Factory.class)
  public static class QueryByAllFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new QueryByAllHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
