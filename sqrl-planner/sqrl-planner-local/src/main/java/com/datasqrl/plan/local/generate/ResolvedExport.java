package com.datasqrl.plan.local.generate;

import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.VirtualRelationalTable;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class ResolvedExport {
  VirtualRelationalTable table;
  RelNode relNode;
  TableSink sink;

  public SQRLConverter.Config getBaseConfig() {
    return SQRLConverter.Config.builder()
        .setOriginalFieldnames(true).build();
  }

}