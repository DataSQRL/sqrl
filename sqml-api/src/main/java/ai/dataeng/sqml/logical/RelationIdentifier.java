package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.tree.QualifiedName;
import java.util.Objects;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class RelationIdentifier {
  QualifiedName name;

  public QualifiedName getName() {
    return name;
  }
}
