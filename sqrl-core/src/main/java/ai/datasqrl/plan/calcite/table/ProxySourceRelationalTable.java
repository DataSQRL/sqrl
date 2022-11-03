package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.pipeline.ExecutionStage;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

public abstract class ProxySourceRelationalTable extends QueryRelationalTable {

    public ProxySourceRelationalTable(@NonNull Name rootTableId, @NonNull TableType type, RelNode relNode, PullupOperator.Container pullups, TimestampHolder.@NonNull Base timestamp, @NonNull int numPrimaryKeys, @NonNull TableStatistic stats, @NonNull ExecutionStage execution) {
        super(rootTableId, type, relNode, pullups, timestamp, numPrimaryKeys, stats, execution);
    }

    public abstract AbstractRelationalTable getBaseTable();

}
