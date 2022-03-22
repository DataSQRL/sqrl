//package ai.dataeng.sqml.planner.operator;
//
//import ai.dataeng.sqml.planner.LogicalPlanImpl;
//import ai.dataeng.sqml.planner.LogicalPlanUtil;
//import ai.dataeng.sqml.planner.Relationship;
//import ai.dataeng.sqml.planner.Table;
//import com.google.common.base.Preconditions;
//
//import java.util.HashSet;
//import java.util.Set;
//
///**
// * Adds {@link LogicalPlanImpl.Node} to a {@link LogicalPlanImpl} to represent the query workload in the logical plan.
// */
//public class QueryAnalyzer {
//
//
//    /**
//     * In development mode, we don't have a fixed set of queries to analyze. Instead, we assume
//     * that every non-hidden table defined in the root of the script can be queried for any of its fields.
//     *
//     * Hence, we add a {@link AccessNode} for all such tables and any tables that are reachable from these
//     * tables via relationships.
//     *
//     * @param logicalPlan
//     */
//    public static final void addDevModeQueries(LogicalPlanImpl logicalPlan) {
//        final Set<Table> included = new HashSet<>();
//        final Set<Table> toInclude = new HashSet<>();
//
//        logicalPlan.schema.visibleStream().filter(t -> t instanceof Table)
//                .map(t -> (Table)t).forEach(t -> toInclude.add(t));
//
//        while (!toInclude.isEmpty()) {
//            Table next = toInclude.iterator().next();
//            assert !included.contains(next);
//            included.add(next);
//            toInclude.remove(next);
//            //Find all non-hidden related tables and add those
//            next.fields.visibleStream().filter(f -> f instanceof Relationship && !f.name.isHidden())
//                    .map(f -> (Relationship)f)
//                    .forEach(r -> {
//                        Preconditions.checkArgument(!r.toTable.name.isHidden(),"Hidden tables should not be reachable by non-hidden relationships: " + r.toTable.name);
//                        if (!included.contains(r.toTable)) {
//                            toInclude.add(r.toTable);
//                        }
//                    });
//        }
//
//        for (Table queryTable : included) {
//            assert queryTable.currentNode!=null;
//            AccessNode query = AccessNode.forEntireTable(queryTable, AccessNode.Type.QUERY);
//            LogicalPlanUtil.appendOperatorForTable(queryTable, query);
//        }
//    }
//
//
//}
