package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
public class TopNConstraint implements PullupOperator {

    public static TopNConstraint EMPTY = new TopNConstraint(List.of(),false,RelCollations.EMPTY,Optional.empty(), TableType.STREAM);

    List<Integer> partition; //First, we partition in the input relation
    boolean distinct; //second, we select distinct rows if true [All columns not in the partition indexes are SELECT DISTINCT columns]
    RelCollation collation; //third, we sort it
    Optional<Integer> limit; //fourth, we limit the result
    TableType inputTableType; //table type of the input relation so we can add a hint in case we are stacking temporal state

    public TopNConstraint(List<Integer> partition, boolean distinct, RelCollation collation, Optional<Integer> limit, TableType inputTableType) {
        this.partition = partition;
        this.distinct = distinct;
        this.collation = collation;
        this.limit = limit;
        this.inputTableType = inputTableType;
        Preconditions.checkArgument(isEmpty() || distinct || limit.isPresent());
    }

    public boolean isEmpty() {
        return !distinct && !hasLimit() && !hasCollation() && !hasPartition();
    }

    public boolean hasPartition() {
        return !partition.isEmpty();
    }

    public boolean hasCollation() {
        return !collation.getFieldCollations().isEmpty();
    }

    public boolean hasLimit() {
        return limit.isPresent();
    }

    public int getLimit() {
        Preconditions.checkArgument(hasLimit());
        return limit.get();
    }

    public TopNConstraint remap(IndexMap map) {
        if (isEmpty()) return this;
        RelCollation newCollation = SqrlRexUtil.mapCollation(collation,map);
        List<Integer> newPartition = partition.stream().map(i -> map.map(i)).collect(Collectors.toList());
        return new TopNConstraint(newPartition, distinct, newCollation, limit, inputTableType);
    }
    
    public List<Integer> getIndexes() {
        return Stream.concat(collation.getFieldCollations().stream().map(c -> c.getFieldIndex()),
                partition.stream()).collect(Collectors.toList());
    }

    public static TopNConstraint dedup(List<Integer> partitionByIndexes, int timestampIndex, TableType inputTableType) {
        RelCollation collation = RelCollations.of(new RelFieldCollation(timestampIndex, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
        return new TopNConstraint(partitionByIndexes,false,collation,Optional.of(1), inputTableType);
    }

    public boolean isPrimaryKeyDedup() {
        return (!distinct && hasLimit() && getLimit()==1) ||
                (distinct && !hasPartition() && !hasLimit());
    }

    public TableType getTableType() {
        if (inputTableType==TableType.STATE) return inputTableType;
        if (!hasLimit() || (hasPartition() && getLimit()==1)) {
            return TableType.TEMPORAL_STATE;
        }
        return TableType.STATE;
    }

}
