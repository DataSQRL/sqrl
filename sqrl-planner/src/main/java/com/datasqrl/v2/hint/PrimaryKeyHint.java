package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.google.auto.service.AutoService;
import java.util.List;

public class PrimaryKeyHint extends PlannerHint {

  public static final String HINT_NAME = "primary_key";

  protected PrimaryKeyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER);
  }

  public List<String> getPkColumns() {
    return getOptions();
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
