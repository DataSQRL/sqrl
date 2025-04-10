package com.datasqrl.v2.hint;

import com.datasqrl.v2.hint.PlannerHint.Type;
import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import java.util.List;

public interface Hint {

  ParsedObject<SqrlHint> getSource();
  Type getType();
  List<String> getOptions();
  String getName();

}
