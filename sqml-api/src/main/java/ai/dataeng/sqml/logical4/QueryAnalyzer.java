package ai.dataeng.sqml.logical4;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

/**
 * Adds {@link LogicalPlan.Node} to a {@link LogicalPlan} to represent the query workload in the logical plan.
 */
public class QueryAnalyzer {


    /**
     * In development mode, we don't have a fixed set of queries to analyze. Instead, we assume
     * that every non-hidden table defined in the root of the script can be queried for any of its fields.
     *
     * Hence, we add a {@link AccessNode} for all such tables and any tables that are reachable from these
     * tables via relationships.
     *
     * @param logicalPlan
     */
    public static final void addDevModeQueries(LogicalPlan logicalPlan) {
        final Set<LogicalPlan.Table> included = new HashSet<>();
        final Set<LogicalPlan.Table> toInclude = new HashSet<>();

        logicalPlan.schema.visibleStream().filter(t -> t instanceof LogicalPlan.Table)
                .map(t -> (LogicalPlan.Table)t).forEach(t -> toInclude.add(t));

        while (!toInclude.isEmpty()) {
            LogicalPlan.Table next = toInclude.iterator().next();
            assert !included.contains(next);
            included.add(next);
            toInclude.remove(next);
            //Find all non-hidden related tables and add those
            next.fields.visibleStream().filter(f -> f instanceof LogicalPlan.Relationship && !f.name.isHidden())
                    .map(f -> (LogicalPlan.Relationship)f)
                    .forEach(r -> {
                        Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships");
                        if (!included.contains(r.toTable)) {
                            toInclude.add(r.toTable);
                        }
                    });
        }

        for (LogicalPlan.Table queryTable : included) {
            assert queryTable.currentNode!=null;
            AccessNode query = new AccessNode(queryTable.currentNode);
            LogicalPlanUtil.appendOperatorForTable(queryTable, query);
        }
    }


}
