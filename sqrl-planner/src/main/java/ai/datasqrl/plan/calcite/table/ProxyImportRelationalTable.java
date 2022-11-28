package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.io.sources.stats.TableStatistic;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.pipeline.ExecutionStage;
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
  private final ImportedRelationalTable baseTable;

  public ProxyImportRelationalTable(@NonNull Name rootTableId, @NonNull TimestampHolder.Base timestamp,
                                    RelNode relNode, ImportedRelationalTable baseTable, ExecutionStage execution,
                                    TableStatistic tableStatistic) {
    super(rootTableId, TableType.STREAM, relNode, PullupOperator.Container.EMPTY, timestamp,
            1,
            tableStatistic,
            execution);
    this.baseTable = baseTable;
  }

}
