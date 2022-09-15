package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value
public class Deduplication implements PullupOperator {

    public static Deduplication EMPTY = new Deduplication(List.of(),-1);

    final List<Integer> partitionByIndexes;
    final int timestampIndex;

    public boolean isEmpty() {
        return timestampIndex<0;
    }

    public boolean hasPartition() {
        return !partitionByIndexes.isEmpty();
    }

    public Deduplication remap(IndexMap map) {
        if (this==EMPTY) return EMPTY;
        List<Integer> newPartition = partitionByIndexes.stream().map(i -> map.map(i)).collect(Collectors.toList());
        int newTimestampIndex = map.map(timestampIndex);
        return new Deduplication(newPartition,newTimestampIndex);
    }

}
