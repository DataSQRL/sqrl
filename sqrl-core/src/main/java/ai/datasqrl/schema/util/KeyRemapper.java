package ai.datasqrl.schema.util;

import static ai.datasqrl.plan.util.FlinkSchemaUtil.getIndex;

import ai.datasqrl.schema.Table;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;

public class KeyRemapper {

  public static Set<Integer> remapPrimary(Table table, RelNode expanded) {
    return table.getPrimaryKey().stream()
        .map(i->table.getRelNode().getRowType().getFieldList().get(i))
        .map(f->getIndex(expanded.getRowType(), f.getName()))
        .collect(Collectors.toSet());
  }

  public static Set<Integer> remapParentPrimary(Table table, RelNode expanded) {
    return table.getParentPrimaryKey().stream()
        .map(i->table.getRelNode().getRowType().getFieldList().get(i))
        .map(f->getIndex(expanded.getRowType(), f.getName()))
        .collect(Collectors.toSet());
  }
}
