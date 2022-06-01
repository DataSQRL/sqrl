package ai.datasqrl.parse.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Hint extends Node {
  String value;

  public Hint(Optional<NodeLocation> location, String value) {
    super(location);
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Hint hint = (Hint) o;
    return Objects.equals(value, hint.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
