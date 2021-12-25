package org.apache.calcite.prepare;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

@Getter
public class UnflattenToTree<T> {
  final T name;
  List<UnflattenToTree> children = new ArrayList();

  public UnflattenToTree(T name) {
    this.name = name;
  }

  public void append(NamePath path) {
    if (path.isEmpty()) {
      return;
    }
    Optional<UnflattenToTree> child = findChild(path.getFirst());
    if (child.isPresent()) {
      child.get().append(path.popFirst());
    } else {
      UnflattenToTree join = new UnflattenToTree(path.getFirst());
      children.add(join);
      join.append(path.popFirst());
    }
  }

  private Optional<UnflattenToTree> findChild(Name name) {
    for (UnflattenToTree join : children) {
      if (join.getName().equals(name)) {
        return Optional.of(join);
      }
    }
    return Optional.empty();
  }
  public static class UnflattenToTreeRoot<T> extends UnflattenToTree<T> {
    public UnflattenToTreeRoot() {
      super(null);
    }
  }
}