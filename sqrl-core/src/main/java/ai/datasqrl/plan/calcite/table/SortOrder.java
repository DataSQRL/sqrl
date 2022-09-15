package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;

import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO: Pullup sort orders through the logical plan and into the database (or discard if they no longer apply)
 */
@Value
public class SortOrder implements PullupOperator {

    public static SortOrder EMPTY = new SortOrder(List.of(), RelCollations.EMPTY);

    final List<Integer> partitionByIndexes;
    final RelCollation collation;

    public boolean isEmpty() {
        return !hasCollation();
    }

    public boolean hasCollation() {
        return !collation.getFieldCollations().isEmpty();
    }

    public boolean hasPartition() {
        return !partitionByIndexes.isEmpty();
    }

    public SortOrder remap(IndexMap map) {
        if (this==EMPTY) return EMPTY;
        List<Integer> newPartition = partitionByIndexes.stream().map(i -> map.map(i)).collect(Collectors.toList());
        RelCollation newCollation = RelCollations.of(collation.getFieldCollations().stream().map(fc -> fc.withFieldIndex(map.map(fc.getFieldIndex()))).collect(Collectors.toList()));
        return new SortOrder(newPartition,newCollation);
    }

}
