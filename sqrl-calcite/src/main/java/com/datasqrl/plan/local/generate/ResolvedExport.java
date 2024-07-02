package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.io.tables.TableSink;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

@Getter
@AllArgsConstructor
public abstract class ResolvedExport {
  private final String table;
  private final RelNode relNode;
  private final int numFieldSelects;

  @Getter
  public static class External extends ResolvedExport {

    private final TableSink sink;

    public External(String table, RelNode relNode, int numFieldSelects, TableSink sink) {
      super(table, relNode, numFieldSelects);
      this.sink = sink;
    }
  }

  @Getter
  public static class Internal extends ResolvedExport {

    private final SystemBuiltInConnectors connector;
    private final NamePath sinkPath;

    public Internal(String table, NamePath sinkPath, RelNode relNode, int numFieldSelects, SystemBuiltInConnectors connector) {
      super(table, relNode, numFieldSelects);
      this.sinkPath = sinkPath;
      this.connector = connector;
    }
  }


}