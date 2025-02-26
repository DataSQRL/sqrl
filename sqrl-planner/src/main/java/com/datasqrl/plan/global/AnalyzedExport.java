package com.datasqrl.plan.global;

import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.local.generate.ResolvedExport;
import java.util.OptionalInt;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AnalyzedExport {

  String table;
  RelNode relNode;
  OptionalInt numSelects;
  TableSink sink;

  public static AnalyzedExport from(ResolvedExport export) {
    return new AnalyzedExport(
        export.getTable(),
        export.getRelNode(),
        OptionalInt.of(export.getNumFieldSelects()),
        export.getSink());
  }
}
