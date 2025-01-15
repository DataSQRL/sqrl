package com.datasqrl.flinkwrapper.planner;

import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SqrlHint;
import com.google.auto.service.AutoService;

public class PrimaryKeyHint extends PlannerHint {

  public static final String HINT_NAME = "primary_key";

  protected PrimaryKeyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }



  @AutoService(Factory.class)
  public static class PKFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new PrimaryKeyHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
