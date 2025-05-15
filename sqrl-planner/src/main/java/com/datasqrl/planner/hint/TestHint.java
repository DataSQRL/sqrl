package com.datasqrl.planner.hint;

import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.google.auto.service.AutoService;

/**
 * Defines a table to be a test case which means it will only be planned when the user runs
 * test cases
 */
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
