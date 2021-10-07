package ai.dataeng.sqml.imports;

import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableImportObject implements ImportObject {
  Mapping mapping;
  String path;

  @Override
  public <R, C> R accept(ImportVisitor<R, C> visitor, C context) {
    return visitor.visitTable(this, context);
  }
}
