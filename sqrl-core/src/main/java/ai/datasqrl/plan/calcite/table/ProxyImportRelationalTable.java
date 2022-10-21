package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.pipeline.ExecutionStage;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

/**
 * A relational table that is defined by a {@link SourceTableImport}, i.e. the imported data from a
 * {@link ai.datasqrl.io.sources.dataset.SourceTable} in a {@link ai.datasqrl.io.sources.dataset.SourceDataset}.
 *
 * This is a phyiscal relation with a schema that captures the input data.
 */
public class ProxyImportRelationalTable extends QueryRelationalTable {

  @Getter
  private final ImportedSourceTable sourceTable;

  public ProxyImportRelationalTable(@NonNull Name rootTableId, @NonNull TimestampHolder.Base timestamp,
                                    RelNode relNode, ImportedSourceTable sourceTable, ExecutionStage execution) {
    super(rootTableId, TableType.STREAM, relNode, PullupOperator.Container.EMPTY, timestamp,
            1,
            TableStatistic.from(sourceTable.getSourceTableImport().getTable().getStatistics()),
            execution);
    this.sourceTable = sourceTable;
  }

}
