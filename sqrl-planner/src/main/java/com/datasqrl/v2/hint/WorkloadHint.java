package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.google.auto.service.AutoService;

/**
 * Defines a table as a workload which means it is expected to be executed in the database
 * and we don't generate an access function for it.
 * It only exists to indicate how a consumer will query the data for index selection and testing.
 */
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
