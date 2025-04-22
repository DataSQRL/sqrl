package com.datasqrl.plan.local.generate;

import org.apache.calcite.rel.RelNode;

import com.datasqrl.io.tables.TableSink;

import lombok.Value;

@Value
public class ResolvedExport {
  String table;
  RelNode relNode;
  int numFieldSelects;
  TableSink sink;

}