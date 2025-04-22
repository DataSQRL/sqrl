package com.datasqrl.v2.hint;

import java.util.List;

import com.datasqrl.v2.hint.PlannerHint.Type;
import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;

public interface Hint {

  ParsedObject<SqrlHint> getSource();
  Type getType();
  List<String> getOptions();
  String getName();

}
