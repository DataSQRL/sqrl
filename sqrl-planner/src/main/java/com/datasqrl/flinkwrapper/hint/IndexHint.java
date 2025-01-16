package com.datasqrl.flinkwrapper.hint;

import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SqrlHint;
import com.google.auto.service.AutoService;

public class IndexHint extends PlannerHint {

  public static final String HINT_NAME = "index";

  protected IndexHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }

  @AutoService(Factory.class)
  public static class IndexHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new IndexHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
