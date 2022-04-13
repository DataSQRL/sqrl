package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Deprecated
public class DistinctOn extends Node {

  private final List<Identifier> on;

  public DistinctOn(List<Identifier> on) {
    this(Optional.empty(), on);
  }
  public DistinctOn(Optional<NodeLocation> nodeLocation, List<Identifier> on) {
    super(nodeLocation);
    this.on = on;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDistinctOn(this, context);
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
    DistinctOn that = (DistinctOn) o;
    return Objects.equals(on, that.on);
  }

  @Override
  public int hashCode() {
    return Objects.hash(on);
  }

  public List<Identifier> getOn() {
    return on;
  }
}
