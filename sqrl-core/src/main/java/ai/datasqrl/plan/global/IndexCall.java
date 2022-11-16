package ai.datasqrl.plan.global;

import ai.datasqrl.config.util.ArrayUtil;
import ai.datasqrl.plan.calcite.rules.SqrlRelMdRowCount;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.global.IndexDefinition.Type.BTREE;
import static ai.datasqrl.plan.global.IndexDefinition.Type.HASH;

@AllArgsConstructor
@Getter
public class IndexCall {

    public static final String INDEX_NAME = "_index_";

    VirtualRelationalTable table;
    Map<Integer,IndexColumn> indexColumns;
    double relativeFrequency;

    public static IndexCall of(@NonNull VirtualRelationalTable table, @NonNull List<IndexColumn> columns) {
        Preconditions.checkArgument(!columns.isEmpty());
        Map<Integer,IndexColumn> indexCols = new HashMap<>();
        columns.stream().filter(c -> c.getType()== CallType.EQUALITY)
                .distinct().forEach(c -> indexCols.put(c.columnIndex,c));
        columns.stream().filter(c -> c.getType()== CallType.COMPARISON).filter(c -> !indexCols.containsKey(c.columnIndex))
                .distinct().forEach(c -> indexCols.put(c.columnIndex,c));
        return new IndexCall(table,indexCols,1.0);
    }

    public static Optional<IndexCall> of(@NonNull VirtualRelationalTable table, RexNode filter, SqrlRexUtil rexUtil) {
        List<RexNode> conjunctions = rexUtil.getConjunctions(filter);
        List<IndexColumn> columns = new ArrayList<>();
        for (RexNode conj : conjunctions) {
            if (conj instanceof RexCall) {
                RexCall call = (RexCall) conj;
                Set<Integer> inputRefs = rexUtil.findAllInputRefs(call.getOperands());
                CallType indexType = null;
                if (call.isA(SqlKind.EQUALS)) indexType = CallType.EQUALITY;
                else if (call.isA(SqlKind.COMPARISON)) indexType = CallType.COMPARISON;
                if (inputRefs.size()==1 && indexType!=null) {
                    columns.add(new IndexColumn(Iterables.getOnlyElement(inputRefs),indexType));
                }
            }
        }
        if (columns.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(IndexCall.of(table,columns));
        }
    }

    public Set<IndexDefinition> generateIndexCandidates() {
        List<Integer> eqCols = indexColumns.values().stream().filter(c -> c.getType()==CallType.EQUALITY)
                .map(IndexColumn::getColumnIndex).collect(Collectors.toList());
        List<Integer> comparisons = indexColumns.values().stream().filter(c -> c.getType()==CallType.COMPARISON)
                .map(IndexColumn::getColumnIndex).collect(Collectors.toList());
        List<List<Integer>> colPermutations = new ArrayList<>();
        generatePermutations(new int[eqCols.size()+(comparisons.isEmpty()?0:1)],0,eqCols,comparisons,colPermutations);
        EnumSet<IndexDefinition.Type> idxTypes;
        if (comparisons.isEmpty() && eqCols.size()==1) idxTypes = EnumSet.of(HASH, BTREE);
        else idxTypes = EnumSet.of(BTREE);
        Set<IndexDefinition> result = new HashSet<>();
        colPermutations.forEach( cols -> {
            idxTypes.forEach( idxType -> result.add(new IndexDefinition(table,cols,idxType)));
        });
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


    public double getCost(@NonNull IndexDefinition indexDef) {
        List<IndexColumn> coveredCols = new ArrayList<>();

        for (int col : indexDef.getColumns()) {
            IndexColumn idxCol = indexColumns.get(col);
            boolean breaksEqualityChain = idxCol==null;
            if (idxCol != null) {
                if (indexDef.getType().supports(idxCol.type)) {
                    coveredCols.add(idxCol);
                } else {
                    breaksEqualityChain = true;
                }
                if (idxCol.type != CallType.EQUALITY) breaksEqualityChain = true;
            }

            if (breaksEqualityChain && indexDef.getType().hasStrictColumnOrder()) break;
        }
        return SqrlRelMdRowCount.getRowCount(table, coveredCols);
    }

    public Collection<IndexColumn> getColumns() {
        return indexColumns.values();
    }

    @Override
    public String toString() {
        return table.getNameId() +
                getColumns().stream().sorted().map(IndexColumn::getName).collect(Collectors.joining());
    }

    @Value
    public static class IndexColumn implements Comparable<IndexColumn> {

        int columnIndex;
        CallType type;

        public String getName() {
            return columnIndex + type.shortForm;
        }

        public IndexColumn withType(CallType type) {
            return new IndexColumn(columnIndex, type);
        }

        @Override
        public int compareTo(@NotNull IndexCall.IndexColumn o) {
            return Integer.compare(columnIndex,o.columnIndex);
        }
    }


    public enum CallType {

        EQUALITY("e"), COMPARISON("i");

        private final String shortForm;

        CallType(String shortForm) {
            this.shortForm = shortForm;
        }
    }

}
