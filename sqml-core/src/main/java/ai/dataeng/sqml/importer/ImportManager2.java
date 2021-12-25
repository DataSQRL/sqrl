package ai.dataeng.sqml.importer;

import ai.dataeng.sqml.tree.name.NamePath;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ImportManager2 {
  ImportStub importStub;

  public void resolve(NamePath namePath) {
    importStub.importTable(namePath);
  }
}
