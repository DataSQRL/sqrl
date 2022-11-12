package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.Planner;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;

import java.util.List;

import static ai.datasqrl.plan.calcite.OptimizationStage.CONVERT2ENUMERABLE;
import static ai.datasqrl.plan.calcite.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

    private final Planner planner;

    public List<IndexSelection> getIndexSelection(RelNode query) {
        RelNode enumerable = planner.transform(CONVERT2ENUMERABLE, query);
        RelNode optimized = planner.transform(READ_QUERY_OPTIMIZATION, enumerable);
        System.out.println(optimized.explain());
        return List.of();
    }

}
