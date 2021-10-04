package ai.dataeng.sqml.imports;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableImportObject implements ImportObject {
  private String path;
  @Override
  public <R, C> R accept(ImportVisitor<R, C> visitor, C context) {
    return visitor.visitTable(this, context);
  }
}
