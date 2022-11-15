package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.calcite.OptimizationStage.READ_QUERY_OPTIMIZATION;

@AllArgsConstructor
public class IndexSelector {

    private final Planner planner;

    public List<IndexSelection> getIndexSelection(OptimizedDAG.ReadQuery query) {
        RelNode optimized = planner.transform(READ_QUERY_OPTIMIZATION, query.getRelNode());
//        System.out.println(optimized.explain());
        IndexFinder indexFinder = new IndexFinder();
        return indexFinder.find(optimized);
    }

    public Collection<IndexSelection> optimizeIndexes(Collection<IndexSelection> indexes) {
        //Prune down to database indexes and remove duplicates
        return indexes.stream().map(IndexSelection::prune)
                .filter(index -> !index.coveredByPrimaryKey(true))
                .collect(Collectors.toSet());
    }


    class IndexFinder extends RelVisitor {

        private static final int PARAM_OFFSET = 10000;

        List<IndexSelection> indexes = new ArrayList<>();
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
                IndexSelection.of(table,filter.getCondition(),rexUtil).map(indexes::add);
            } else {
                super.visit(node, ordinal, parent);
            }
        }


        private RexNode pushJoinConditionIntoRight(Join join) {
            return join.getCondition().accept(new JoinConditionRewriter(join.getLeft().getRowType().getFieldCount(),
                    join.getRight()));
        }

        List<IndexSelection> find(RelNode node) {
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
