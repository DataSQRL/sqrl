package ai.dataeng.sqml.catalog;

/**
 * Similar to a namespace in code, it helps provide scope for certain identifiers
 */
public interface Namespace {
  /**
   * Supports shadowing of system functions
   */
  void addFunction(String name, Function function);

  Function getFunction(String name);

  Schema getSchema();
}
