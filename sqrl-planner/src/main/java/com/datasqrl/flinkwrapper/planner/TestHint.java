package com.datasqrl.flinkwrapper.planner;

import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SqrlHint;
import com.google.auto.service.AutoService;

public class TestHint extends PlannerHint {

  public static final String HINT_NAME = "test";

  protected TestHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }



  @AutoService(Factory.class)
  public static class TestHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new TestHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
