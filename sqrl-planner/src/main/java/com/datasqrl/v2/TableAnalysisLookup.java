package com.datasqrl.v2;

import com.datasqrl.v2.analyzer.TableAnalysis;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableAnalysisLookup {

  default TableAnalysis lookupTable(List<String> names) {
    Preconditions.checkArgument(names.size()==3);
    return lookupSourceTable(ObjectIdentifier.of(names.get(0), names.get(1), names.get(2)));
  }

  TableAnalysis lookupSourceTable(ObjectIdentifier objectId);

  Optional<TableAnalysis> lookupTable(RelNode originalRelnode);


}
