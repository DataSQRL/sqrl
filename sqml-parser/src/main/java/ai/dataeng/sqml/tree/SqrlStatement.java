package ai.dataeng.sqml.tree;

import java.util.Optional;

public abstract class SqrlStatement extends Node {

  protected SqrlStatement(Optional<NodeLocation> location) {
    super(location);
  }
}
