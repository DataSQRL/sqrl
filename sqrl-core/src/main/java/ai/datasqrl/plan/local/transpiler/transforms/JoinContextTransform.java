package ai.datasqrl.plan.local.transpiler.transforms;

import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Table;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class JoinContextTransform {

  /**
   * Add a context table to the join, if needed.
   */
  public static Relation addContextTable(Relation from, Optional<Table> context, boolean isSelfInScope) {
    boolean hasAnyContextReference = hasAnyContextReference(from);
    if (isSelfInScope || context.isEmpty() || hasAnyContextReference) {
      return from;
    }

    //Special case: if we're nested then we need to do a cross join on the first table
    //e.g. _ JOIN Orders
    return new Join(Optional.empty(), Type.CROSS,
        new TableNode(Optional.empty(), Name.SELF_IDENTIFIER.toNamePath(),
            Optional.of(Name.SELF_IDENTIFIER)), from, Optional.empty());
  }

  private static boolean hasAnyContextReference(Relation node) {
    AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    node.accept(new DefaultTraversalVisitor<>() {
      @Override
      public Object visitTableNode(TableNode node, Object context) {
        if (node.getNamePath().getFirst().equals(Name.SELF_IDENTIFIER)) {
          atomicBoolean.set(true);
        }
        return null;
      }
    }, null);

    return atomicBoolean.get();
  }
}
