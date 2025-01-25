package com.datasqrl.flinkwrapper.hint;

import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SqrlHint;
import com.google.auto.service.AutoService;

public class WorkloadHint extends PlannerHint {

  public static final String HINT_NAME = "workload";

  protected WorkloadHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }


  @AutoService(Factory.class)
  public static class WorkLoadHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new WorkloadHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
