package ai.dataeng.sqml.function;

import ai.dataeng.sqml.tree.QualifiedName;
import java.util.ArrayList;
import java.util.List;

public class FunctionProvider {

  private final List<SqmlFunction> functions;

  public FunctionProvider(List<SqmlFunction> functions) {
    this.functions = functions;
  }

  public static Builder newFunctionProvider() {
    return new Builder();
  }

  public SqmlFunction resolve(QualifiedName name) {

    return null;
  }

  public static class Builder {
    private List<SqmlFunction> functions = new ArrayList<>();
    public Builder function(List<SqmlFunction> functions) {
      this.functions.addAll(functions);
      return this;
    }

    public Builder function(SqmlFunction function) {
      this.functions.add(function);
      return this;
    }

    public FunctionProvider build() {
      return new FunctionProvider(this.functions);
    }
  }
}
