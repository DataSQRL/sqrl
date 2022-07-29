package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;

/**
 * An extension of {@link QueryRelationalTable} that separates out a non-empty {@link TopNConstraint} so that we push
 * that constraint into the database if this table is not accessed inside the write-DAG.
 *
 * The base relation is the relation of this table without the topN constraint whereas the relnode of the extended
 * {@link QueryRelationalTable} is the relation with the topN constraint inlined.
 */
@Getter
public class TopNRelationalTable extends QueryRelationalTable {

    @NonNull
    private final TopNConstraint topN;
    @NonNull
    private RelNode baseRel;

    @Setter
    private boolean inlinedTopN = true;

    public TopNRelationalTable(@NonNull Name rootTableId, @NonNull Type type, RelNode relNode,
                               TimestampHolder.@NonNull Base timestamp, @NonNull int numPrimaryKeys,
                               @NonNull TopNConstraint topN, @NonNull RelNode baseRel) {
        super(rootTableId, type, relNode, timestamp, numPrimaryKeys);
        Preconditions.checkArgument(!topN.isEmpty());
        this.topN = topN;
        this.baseRel = baseRel;
    }

    public void setOptimizedBaseRel(@NonNull RelNode baseRel) {
        this.baseRel = baseRel;
    }
}
