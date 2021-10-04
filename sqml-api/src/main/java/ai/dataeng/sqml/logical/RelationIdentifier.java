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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationIdentifier relationIdentifier = (RelationIdentifier) o;
    return Objects.equals(name, relationIdentifier.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
