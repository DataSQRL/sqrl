package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.plan.calcite.table.ImportedRelationalTable;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

public class ExtractUniqueSourceVisitor extends RelVisitor {

  @Getter
  Map<String, ImportedRelationalTable> tableMap = new HashMap<>();

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    if (node instanceof TableScan) {
      TableScan tableScan = (TableScan) node;
      ImportedRelationalTable table = tableScan.getTable().unwrap(ImportedRelationalTable.class);
      tableMap.put(table.getNameId(), table);
    }

    super.visit(node, ordinal, parent);
  }

  public void extractFrom(RelNode relNode) {
    go(relNode);
  }
}