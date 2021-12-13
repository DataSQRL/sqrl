package ai.dataeng.sqml.function.definition;

public enum FunctionKind {
  SCALAR,
  AGGREGATE,
  OTHER;

  private FunctionKind() {
  }
}