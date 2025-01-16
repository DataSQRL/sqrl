package com.datasqrl.flinkwrapper.planner;

import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SqrlHint;
import com.google.auto.service.AutoService;

public class PartitionKeyHint extends PlannerHint {

  public static final String HINT_NAME = "partition_key";

  protected PartitionKeyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER);
  }

  @AutoService(Factory.class)
  public static class PartitionKeyFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new PartitionKeyHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
