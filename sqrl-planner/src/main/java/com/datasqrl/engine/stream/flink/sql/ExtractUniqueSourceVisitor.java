package com.datasqrl.engine.stream.flink.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.table.ImportedRelationalTable;

import lombok.Getter;

public class ExtractUniqueSourceVisitor extends RelVisitor {

  @Getter
  Map<String, ImportedRelationalTable> tableMap = new HashMap<>();

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    if (node instanceof TableScan tableScan) {
      ImportedRelationalTable table = tableScan.getTable().unwrap(ImportedRelationalTable.class);
      tableMap.put(table.getNameId(), table);
    }

    super.visit(node, ordinal, parent);
  }

  public void extractFrom(RelNode relNode) {
    go(relNode);
  }

  public Map<String, ImportedRelationalTable> extract(List<WriteQuery> queries) {
    for (WriteQuery query : queries) {
      extractFrom(query.getExpandedRelNode());
    }

    return getTableMap();
  }
}