package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSink;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
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

    public Internal(String table, RelNode relNode, int numFieldSelects, SystemBuiltInConnectors connector) {
      super(table, relNode, numFieldSelects);
      this.connector = connector;
    }
  }


}