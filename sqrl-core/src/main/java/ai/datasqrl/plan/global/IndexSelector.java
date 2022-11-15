package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import lombok.AllArgsConstructor;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.*;

import static ai.datasqrl.plan.calcite.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

    public static final double DEFAULT_COST_THRESHOLD = 0.95;

    private final Planner planner;
    private final double costImprovementThreshold = DEFAULT_COST_THRESHOLD;

    public List<IndexCall> getIndexSelection(OptimizedDAG.ReadQuery query) {
        RelNode optimized = planner.transform(READ_QUERY_OPTIMIZATION, query.getRelNode());
//        System.out.println(optimized.explain());
        IndexFinder indexFinder = new IndexFinder();
        return indexFinder.find(optimized);
    }

    public Map<IndexDefinition, Double> optimizeIndexes(Collection<IndexCall> indexes) {
        //Prune down to database indexes and remove duplicates
        Map<IndexDefinition, Double> optIndexes = new HashMap<>();
        Multimap<VirtualRelationalTable,IndexCall> callsByTable = HashMultimap.create();
        indexes.forEach(idx -> callsByTable.put(idx.getTable(),idx));

        for (VirtualRelationalTable table : callsByTable.keySet()) {
            optIndexes.putAll(optimizeIndexes(table,callsByTable.get(table)));
        }
        return optIndexes;
    }

    private Map<IndexDefinition, Double> optimizeIndexes(VirtualRelationalTable table, Collection<IndexCall> indexes) {
        Map<IndexDefinition, Double> optIndexes = new HashMap<>();
        //The baseline cost is the cost of doing the lookup with the primary key index
        Map<IndexCall, Double> currentCost = new HashMap<>();
        IndexDefinition pkIdx = IndexDefinition.getPrimaryKeyIndex(table);
        for (IndexCall idx : indexes) {
            currentCost.put(idx, idx.getCost(pkIdx));
        }
        //Determine which index candidates reduce the cost the most
        Set<IndexDefinition> candidates = new HashSet<>();
        indexes.forEach( idx -> candidates.addAll(idx.generateIndexCandidates()));
        candidates.remove(pkIdx);
        double beforeTotal = total(currentCost);
        for (;;) {
            IndexDefinition bestCandidate = null;
            Map<IndexCall, Double> bestCosts = null;
            double bestTotal = Double.POSITIVE_INFINITY;
            for (IndexDefinition candidate : candidates) {
                Map<IndexCall, Double> costs = new HashMap<>();
                currentCost.forEach( (call, cost) -> {
                    double newcost = call.getCost(candidate);
                    if (newcost>cost) newcost = cost;
                    costs.put(call, newcost);
                });
                double total = total(costs);
                if (total<beforeTotal && total<bestTotal) {
                    bestCandidate = candidate;
                    bestCosts = costs;
                    bestTotal = total;
                }
            }
            if (bestCandidate!=null && bestTotal/beforeTotal <= costImprovementThreshold ) {
                optIndexes.put(bestCandidate,beforeTotal-bestTotal);
                candidates.remove(bestCandidate);
                beforeTotal = bestTotal;
                currentCost = bestCosts;
            } else {
                break;
            }
        }
        return optIndexes;
    }

    private static final double total(Map<?,Double> costs) {
        return costs.values().stream().reduce(0.0d, (a,b) -> a+b );
    }


    class IndexFinder extends RelVisitor {

        private static final int PARAM_OFFSET = 10000;

        List<IndexCall> indexes = new ArrayList<>();
        int paramIndex = PARAM_OFFSET;
        SqrlRexUtil rexUtil = new SqrlRexUtil(planner.getTypeFactory());

        @Override public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof EnumerableNestedLoopJoin) {
                EnumerableNestedLoopJoin join = (EnumerableNestedLoopJoin)node;
                visit(join.getLeft(),0,node);
                RelNode right = join.getRight();
                //Push join filter into right
                RexNode nestedCondition = pushJoinConditionIntoRight(join);
                right = EnumerableFilter.create(right,nestedCondition);
                right = planner.transform(OptimizationStage.PUSH_DOWN_FILTERS,right);
                visit(right,1,node);
            } else if (node instanceof TableScan && parent instanceof Filter) {
                VirtualRelationalTable table = ((TableScan)node).getTable().unwrap(VirtualRelationalTable.class);
                Filter filter = (Filter)parent;
                IndexCall.of(table,filter.getCondition(),rexUtil).map(indexes::add);
            } else {
                super.visit(node, ordinal, parent);
            }
        }


        private RexNode pushJoinConditionIntoRight(Join join) {
            return join.getCondition().accept(new JoinConditionRewriter(join.getLeft().getRowType().getFieldCount(),
                    join.getRight()));
        }

        List<IndexCall> find(RelNode node) {
            go(node);
            return indexes;
        }

        @AllArgsConstructor
        class JoinConditionRewriter extends RexShuttle {

            final int maxLeftIdx;
            final RelNode right;

            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                if (ref.getIndex()<maxLeftIdx) {
                    //Replace with variables
                    return new RexDynamicParam(ref.getType(),paramIndex++);
                } else {
                    //Shift indexes
                    return RexInputRef.of(ref.getIndex()-maxLeftIdx, right.getRowType());
                }
            }

        }

    }



}
