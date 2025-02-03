package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.google.auto.service.AutoService;
import java.util.List;

public class ExecHint extends PlannerHint {

  public static final String HINT_NAME = "exec";

  protected ExecHint(ParsedObject<SqrlHint> source) {
    super(source, Type.DAG);
  }

  public List<String> getStageNames() {
    return super.getOptions();
  }

  @AutoService(Factory.class)
  public static class ExecHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new ExecHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
