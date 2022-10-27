package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.pipeline.ExecutionStage;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

/**
 * A relational table that is defined by a {@link StreamSourceTable}, i.e. the change stream that is generated
 * from an underlying state table as captured by the {@link RelNode} in the {@code sourceTable}.
 *
 */
public class ProxyStreamRelationalTable extends SourceRelationalTable {

  @Getter
  private final StreamSourceTable baseTable;

  public ProxyStreamRelationalTable(@NonNull Name rootTableId, @NonNull TimestampHolder.Base timestamp,
                                    RelNode relNode, StreamSourceTable baseTable, ExecutionStage execution) {
    super(rootTableId, TableType.STREAM, relNode, PullupOperator.Container.EMPTY, timestamp,
            1,
            TableStatistic.UNKNOWN,
            execution);
    this.baseTable = baseTable;
  }

}
