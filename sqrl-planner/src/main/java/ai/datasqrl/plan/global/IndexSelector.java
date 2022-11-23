package ai.datasqrl.plan.global;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.util.ArrayUtil;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
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
import static ai.datasqrl.plan.global.IndexDefinition.Type.BTREE;
import static ai.datasqrl.plan.global.IndexDefinition.Type.HASH;

@AllArgsConstructor
public class IndexSelector {

    private final Planner planner;
    private final Config config;

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
        indexes.forEach( idx -> candidates.addAll(generateIndexCandidates(idx)));
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
                if (total<beforeTotal && (total<bestTotal ||
                        (total==bestTotal && config.relativeIndexCost(candidate) < config.relativeIndexCost(bestCandidate)))) {
                    bestCandidate = candidate;
                    bestCosts = costs;
                    bestTotal = total;
                }
            }
            if (bestCandidate!=null && bestTotal/beforeTotal <= config.costImprovementThreshold ) {
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

    public Set<IndexDefinition> generateIndexCandidates(IndexCall indexCall) {
        List<Integer> eqCols = new ArrayList<>(), comparisons = new ArrayList<>();
        indexCall.getColumns().forEach(c -> {
            switch (c.getType()) {
                case EQUALITY:
                    eqCols.add(c.getColumnIndex());
                    break;
                case COMPARISON:
                    comparisons.add(c.getColumnIndex());
                    break;
                default:
                    throw new IllegalStateException(c.getType().name());
            }
        });
        Set<IndexDefinition> result = new HashSet<>();

        for (IndexDefinition.Type indexType : config.supportedIndexTypes()) {
            List<List<Integer>> colPermutations = new ArrayList<>();
            switch (indexType) {
                case HASH:
                    generatePermutations(new int[Math.min(eqCols.size(),config.maxIndexColumns(indexType))],
                            0,eqCols,List.of(),colPermutations);
                    break;
                case BTREE:
                    generatePermutations(new int[Math.min(eqCols.size()+(comparisons.isEmpty()?0:1),config.maxIndexColumns(indexType))],
                            0,eqCols,comparisons,colPermutations);
                    break;
                default:
                    throw new IllegalStateException(indexType.name());
            }
            colPermutations.forEach( cols -> result.add(new IndexDefinition(indexCall.getTable(), cols, indexType)));
        }
        return result;
    }


    private void generatePermutations(int[] selected, int depth, List<Integer> eqCols,
                                      List<Integer> comparisons, Collection<List<Integer>> permutations) {
        if (depth>=selected.length) {
            permutations.add(Ints.asList(selected.clone()));
            return;
        }
        if (depth>=eqCols.size()) {
            for (int comp : comparisons) {
                selected[depth] = comp;
                generatePermutations(selected, depth+1, eqCols, comparisons, permutations);
            }
        }
        for (int eq : eqCols) {
            if (ArrayUtil.contains(selected,eq,depth)) continue;
            selected[depth]=eq;
            generatePermutations(selected, depth+1, eqCols, comparisons, permutations);
        }
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

    @Value
    @Builder
    public static class Config {

        public static final double DEFAULT_COST_THRESHOLD = 0.95;
        public static final int MAX_INDEX_COLUMNS = 6;

        @Builder.Default
        double costImprovementThreshold = DEFAULT_COST_THRESHOLD;
        @NonNull
        JDBCConfiguration.Dialect dialect;
        @Builder.Default
        int maxIndexColumns = MAX_INDEX_COLUMNS;

        public EnumSet<IndexDefinition.Type> supportedIndexTypes() {
            switch (dialect) {
                case POSTGRES:
                    return EnumSet.of(HASH, BTREE);
                case MYSQL:
                    return EnumSet.of(HASH, BTREE);
                case H2:
                    return EnumSet.of(HASH, BTREE);
                default:
                    throw new IllegalStateException(dialect.name());
            }
        }

        public int maxIndexColumns(IndexDefinition.Type indexType) {
            switch (dialect) {
                case POSTGRES:
                    switch (indexType) {
                        case HASH: return 1;
                        default: return maxIndexColumns;
                    }
                case MYSQL:
                    return maxIndexColumns;
                case H2:
                    return maxIndexColumns;
                default:
                    throw new IllegalStateException(dialect.name());
            }
        }

        public double relativeIndexCost(IndexDefinition index) {
            double epsilon = epsilon(index.getColumns()); //Add an epsilon that is insignificant but keeps index order stable
            switch (index.getType()) {
                case HASH:
                    return 1+epsilon;
                case BTREE:
                    return 1.5 + 0.1*index.getColumns().size() + epsilon;
                default:
                    throw new IllegalStateException(index.getType().name());
            }
        }

        public static final double epsilon(List<Integer> columns) {
            long eps = 0;
            for (int col : columns) {
                eps = eps*2 + col;
            }
            return eps*1e-10;
        }

        public static Config of(JDBCConnectionProvider jdbc) {
            return Config.builder().dialect(jdbc.getDialect()).build();
        }

    }

}
