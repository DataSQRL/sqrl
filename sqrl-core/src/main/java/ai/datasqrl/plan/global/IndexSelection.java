package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Value
public class IndexSelection {

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

    @Value
    public static class IndexColumn {

        int columnIndex;
        Type type;

    }


    public enum Type {

        EQUALITY, INEQUALITY;

    }

}
