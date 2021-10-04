package ai.dataeng.sqml.function;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.UnknownType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FunctionProvider {

  private final List<SqmlFunction> functions;

  public FunctionProvider(List<SqmlFunction> functions) {
    this.functions = new ArrayList<>(functions);
  }

  public static Builder newFunctionProvider() {
    return new Builder();
  }

  public Optional<SqmlFunction> resolve(QualifiedName name) {
    for (SqmlFunction function : functions) {
      if (function.getName().equals(name.toString())) {
        return Optional.of(function);
      }
    }
    System.out.println("Missing function " + name);
    return Optional.of(
        new SqmlFunction(name.toString(), new UnknownType(), false));
//    return Optional.empty();
  }

  public void add(String name, SqmlFunction function) {
    functions.add(function);
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
