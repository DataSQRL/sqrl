package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Value
public class TopNConstraint {

    public static TopNConstraint EMPTY = new TopNConstraint(RelCollations.EMPTY,List.of(),Optional.empty(),false);

    RelCollation collation;
    List<Integer> partitionByIndexes;
    Optional<Integer> limit;
    boolean distinct; //All columns not in the partition indexes are SELECT DISTINCT columns if distinct is true

    public boolean isEmpty() {
        return !distinct && !hasLimit() && !hasCollation() && !hasPartition();
    }

    public boolean hasPartition() {
        return !partitionByIndexes.isEmpty();
    }

    public boolean hasCollation() {
        return !collation.getFieldCollations().isEmpty();
    }

    public boolean hasLimit() {
        return limit.isPresent();
    }

    public TopNConstraint remap(IndexMap map) {
        RelCollation newCollation = RelCollations.of(collation.getFieldCollations().stream().map(fc -> fc.withFieldIndex(map.map(fc.getFieldIndex()))).collect(Collectors.toList()));
        List<Integer> newPartition = partitionByIndexes.stream().map(i -> map.map(i)).collect(Collectors.toList());
        return new TopNConstraint(newCollation, newPartition, limit, distinct);
    }

}
