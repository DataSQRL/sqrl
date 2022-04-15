package ai.datasqrl.validate;

import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.Query;
import java.util.List;

public class PrimaryKeyDeriver extends DefaultTraversalVisitor<Void, Void> {

  public List<Integer> derive(Query query) {

    return List.of(0);
  }
}
