package org.apache.calcite.schema;

import ai.datasqrl.parse.tree.name.VersionedName;
import ai.datasqrl.plan.nodes.SqrlViewTable;
import ai.datasqrl.schema.Relationship;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SqrlCalciteSchema extends AbstractSqrlSchema {
  ai.datasqrl.schema.Schema schema;
  @Override
  public Table getTable(String s) {
    for (ai.datasqrl.schema.Table table : getAllTables(schema)) {
      if (VersionedName.of(table.getName(), table.getVersion()).getCanonical().equalsIgnoreCase(s)) {
        return new SqrlViewTable(table.getRelNode().getRowType(), table.getRelNode());
      }
    }

    throw new RuntimeException("All tables are registered to calcite through the catalog " + s);
  }

  public Set<ai.datasqrl.schema.Table> getAllTables(ai.datasqrl.schema.Schema schema) {
    final Set<ai.datasqrl.schema.Table> included = new HashSet<>();
    final Set<ai.datasqrl.schema.Table> toInclude = new HashSet<>();

    for (ai.datasqrl.schema.Table table : schema.visibleList()) {
      toInclude.add(table);
    }

    while (!toInclude.isEmpty()) {
      ai.datasqrl.schema.Table next = toInclude.iterator().next();
      assert !included.contains(next);
      included.add(next);
      toInclude.remove(next);
      //Find all non-hidden related tables and add those
      next.getFields().visibleStream().filter(f -> f instanceof Relationship)
          .map(f -> (Relationship)f)
          .forEach(r -> {
            if (!included.contains(r.toTable)) {
              toInclude.add(r.toTable);
            }
          });
    }
    return included;
  }
}
