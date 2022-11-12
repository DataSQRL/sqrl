package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

    public IndexSelection prune() {
        if (remainingIndexColumns.size()<2) return this;
        return new IndexSelection(table,equalityIndexColumns,List.of(remainingIndexColumns.get(0)));
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

    }


    public enum Type {

        EQUALITY("e"), INEQUALITY("i");

        private final String shortForm;

        Type(String shortForm) {
            this.shortForm = shortForm;
        }
    }

}
