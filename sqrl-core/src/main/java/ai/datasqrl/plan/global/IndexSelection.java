package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Iterables;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.*;
import java.util.stream.Collectors;

@Value
public class IndexSelection {

    public static final String INDEX_NAME = "_index_";

    VirtualRelationalTable table;
    Set<Integer> equalityIndexColumns;
    List<IndexColumn> remainingIndexColumns;

    public static IndexSelection of(@NonNull VirtualRelationalTable table, @NonNull List<IndexColumn> columns) {
        Preconditions.checkArgument(!columns.isEmpty());
        Set<Integer> equalityCols = columns.stream().filter(c -> c.getType()==Type.EQUALITY)
                .map(IndexColumn::getColumnIndex)
                .collect(Collectors.toSet());
        List<IndexColumn> remaining = columns.stream().filter(c -> c.getType()!=Type.EQUALITY)
                .collect(Collectors.toList());
        return new IndexSelection(table,equalityCols,remaining);
    }

    public static Optional<IndexSelection> of(@NonNull VirtualRelationalTable table, RexNode filter, SqrlRexUtil rexUtil) {
        List<RexNode> conjunctions = rexUtil.getConjunctions(filter);
        List<IndexColumn> columns = new ArrayList<>();
        for (RexNode conj : conjunctions) {
            if (conj instanceof RexCall) {
                RexCall call = (RexCall) conj;
                Set<Integer> inputRefs = rexUtil.findAllInputRefs(call.getOperands());
                Type indexType = null;
                if (call.isA(SqlKind.EQUALS)) indexType = Type.EQUALITY;
                else if (call.isA(SqlKind.COMPARISON)) indexType = Type.INEQUALITY;
                if (inputRefs.size()==1 && indexType!=null) {
                    columns.add(new IndexColumn(Iterables.getOnlyElement(inputRefs),indexType));
                }
            }
        }
        if (columns.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(IndexSelection.of(table,columns));
        }
    }

    public IndexSelection prune() {
        if (remainingIndexColumns.isEmpty()) return this;
        List<IndexColumn> remaining = new ArrayList<>(remainingIndexColumns);
        Iterator<IndexColumn> iter = remaining.iterator();
        while (iter.hasNext()) {
            if (equalityIndexColumns.contains(iter.next().columnIndex)) iter.remove();
        }
        return new IndexSelection(table,equalityIndexColumns,List.of(remaining.get(0)));
    }

    public boolean coveredByPrimaryKey(boolean isBtree) {
        if (remainingIndexColumns.size()>(isBtree?1:0)) return false;
        //The indexed columns must be part of the primary key columns and must be in order starting from first
        List<IndexColumn> columns = getColumns();
        if (columns.size()>table.getNumPrimaryKeys()) return false;
        return columns.stream().map(IndexColumn::getColumnIndex).collect(Collectors.toList())
                .equals(ContiguousSet.closedOpen(0,columns.size()).asList());
    }

    public List<IndexColumn> getColumns() {
        ArrayList<IndexColumn> result = new ArrayList<>();
        equalityIndexColumns.stream().sorted(Integer::compareTo)
                .map(idx -> new IndexColumn(idx,Type.EQUALITY)).forEach(result::add);
        result.addAll(remainingIndexColumns);
        return result;
    }

    public String getName() {
        return table.getNameId() + INDEX_NAME +
                getColumns().stream().map(IndexColumn::getName).collect(Collectors.joining());
    }

    @Value
    public static class IndexColumn {

        int columnIndex;
        Type type;

        public String getName() {
            return String.valueOf(columnIndex) + type.shortForm;
        }

        public IndexColumn withType(Type type) {
            return new IndexColumn(columnIndex, type);
        }

    }


    public enum Type {

        EQUALITY("e"), INEQUALITY("i");

        private final String shortForm;

        Type(String shortForm) {
            this.shortForm = shortForm;
        }
    }

}
