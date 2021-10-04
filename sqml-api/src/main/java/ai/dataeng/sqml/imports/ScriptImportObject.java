package ai.dataeng.sqml.imports;

import lombok.Getter;

@Getter
public class ScriptImportObject implements ImportObject {
  private String path;
  @Override
  public <R, C> R accept(ImportVisitor<R, C> visitor, C context) {
    return visitor.visitScript(this, context);
  }
}
