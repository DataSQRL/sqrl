package com.datasqrl.v2;

import com.datasqrl.v2.analyzer.TableAnalysis;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Value
public class TableAnalysisLookup {

  Map<ObjectIdentifier, TableAnalysis> sourceTableMap = new HashMap<>();
  HashMultimap<Integer, TableAnalysis> tableMap = HashMultimap.create();
  Map<ObjectIdentifier, TableAnalysis> id2Table = new HashMap<>();


  public TableAnalysis lookupSourceTable(ObjectIdentifier objectId) {
    return sourceTableMap.get(objectId);
  }

  public Optional<TableAnalysis> lookupTable(RelNode originalRelnode) {
    int hashCode = originalRelnode.deepHashCode();
    return tableMap.get(hashCode).stream().filter(tbl -> tbl.matches(originalRelnode)).findFirst();
  }

  public TableAnalysis lookupTable(ObjectIdentifier objectIdentifier) {
    return id2Table.get(objectIdentifier);
  }

  public void registerTable(TableAnalysis tableAnalysis) {
    if (tableAnalysis.isSourceOrSink()) {
      sourceTableMap.put(tableAnalysis.getIdentifier(), tableAnalysis);
    } else {
      tableMap.put(tableAnalysis.getOriginalRelnode().deepHashCode(), tableAnalysis);
    }
    id2Table.put(tableAnalysis.getIdentifier(), tableAnalysis);
  }
}
