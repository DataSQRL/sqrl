package ai.dataeng.sqml.imports;

public abstract class ImportVisitor<R, C>  {

  public R visitScript(ScriptImportObject object, C context) {
    return null;
  }

  public R visitTable(TableImportObject object, C context) {
    return null;
  }

  public R visitFunction(FunctionImportObject object, C context) {
    return null;
  }
}
