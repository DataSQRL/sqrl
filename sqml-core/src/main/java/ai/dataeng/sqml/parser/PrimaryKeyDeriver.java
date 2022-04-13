package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Query;
import java.util.List;

public class PrimaryKeyDeriver extends DefaultTraversalVisitor<Void, Void> {

  public List<Integer> derive(Query query) {

    return List.of(0);
  }
}
