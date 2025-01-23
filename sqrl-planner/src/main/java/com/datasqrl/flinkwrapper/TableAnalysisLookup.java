package com.datasqrl.flinkwrapper;

import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableAnalysisLookup {

  TableAnalysis lookupSourceTable(ObjectIdentifier objectId);

  Optional<TableAnalysis> lookupTable(RelNode relNode);


}
