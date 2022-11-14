package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.util.RelToSql;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.calcite.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

    private final Planner planner;

    public List<IndexSelection> getIndexSelection(OptimizedDAG.ReadQuery query) {
        RelNode optimized = planner.transform(READ_QUERY_OPTIMIZATION, query.getRelNode());
        System.out.println(RelToSql.convertToSql(optimized));
        System.out.println(optimized.explain());
        return List.of();
    }

    public Collection<IndexSelection> optimizeIndexes(Collection<IndexSelection> indexes) {
        //Prune down to database indexes and remove duplicates
        return indexes.stream().map(IndexSelection::prune)
                .filter(index -> !index.coveredByPrimaryKey(true))
                .collect(Collectors.toSet());
    }

}
