package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.NamePath;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ImportManager {
  ImportStub importStub;

  public void resolve(NamePath namePath) {
    importStub.importTable(namePath);
  }
}
