package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.Planner;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.calcite.OptimizationStage.CONVERT2ENUMERABLE;
import static ai.datasqrl.plan.calcite.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

    private final Planner planner;

    public List<IndexSelection> getIndexSelection(OptimizedDAG.ReadQuery query) {
        RelNode enumerable = planner.transform(CONVERT2ENUMERABLE, query.getRelNode());
        RelNode optimized = planner.transform(READ_QUERY_OPTIMIZATION, enumerable);
        System.out.println(optimized.explain());
        return List.of();
    }

    public Collection<IndexSelection> optimizeIndexes(Collection<IndexSelection> indexes) {
        //Prune down to database indexes and remove duplicates
        return indexes.stream().map(IndexSelection::prune).collect(Collectors.toSet());
    }

}
