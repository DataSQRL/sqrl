package com.datasqrl.plan.local.generate;

import com.datasqrl.io.tables.TableSink;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class ResolvedExport {
  String table;
  RelNode relNode;
  int numFieldSelects;
  TableSink sink;

}