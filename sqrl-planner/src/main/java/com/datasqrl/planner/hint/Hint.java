package com.datasqrl.planner.hint;

import java.util.List;

import com.datasqrl.planner.hint.PlannerHint.Type;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;

public interface Hint {

  ParsedObject<SqrlHint> getSource();
  Type getType();
  List<String> getOptions();
  String getName();

}
