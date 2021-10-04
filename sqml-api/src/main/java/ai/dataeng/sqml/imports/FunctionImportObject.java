package ai.dataeng.sqml.imports;

import ai.dataeng.sqml.function.SqmlFunction;
import lombok.Getter;

@Getter
public class FunctionImportObject implements ImportObject {

  private final SqmlFunction function;
  private final String name;
  private String path;

  public FunctionImportObject(SqmlFunction function, String name, String path) {
    this.function = function;
    this.name = name;
    this.path = path;
  }

  @Override
  public <R, C> R accept(ImportVisitor<R, C> visitor, C context) {
    return visitor.visitFunction(this, context);
  }
}
