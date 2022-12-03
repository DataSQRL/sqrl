package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.stats.TableStatistic;
import com.datasqrl.name.Name;
import com.datasqrl.physical.pipeline.ExecutionStage;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

/**
 * A relational table that is defined by the imported data from a
 * {@link TableSource}.
 *
 * This is a phyiscal relation with a schema that captures the input data.
 */
public class ProxyImportRelationalTable extends ProxySourceRelationalTable {

  @Getter
  private final ImportedRelationalTableImpl baseTable;

  public ProxyImportRelationalTable(@NonNull Name rootTableId, @NonNull TimestampHolder.Base timestamp,
                                    RelNode relNode, ImportedRelationalTableImpl baseTable, ExecutionStage execution,
                                    TableStatistic tableStatistic) {
    super(rootTableId, TableType.STREAM, relNode, PullupOperator.Container.EMPTY, timestamp,
            1,
            tableStatistic,
            execution);
    this.baseTable = baseTable;
  }

}
