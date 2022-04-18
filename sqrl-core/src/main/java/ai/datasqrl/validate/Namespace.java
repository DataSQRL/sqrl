package ai.datasqrl.validate;

import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.parse.tree.name.NamePath;

public class Namespace {

  public SqrlFunction resolveFunction(NamePath namePath) {
    FunctionLookup functionLookup = new FunctionLookup();
    SqrlFunction function = functionLookup.lookup(namePath);

    return function;
  }
}
