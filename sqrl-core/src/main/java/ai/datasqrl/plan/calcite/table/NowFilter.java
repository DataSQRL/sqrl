package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import ai.datasqrl.plan.calcite.util.TimePredicate;
import com.google.common.base.Preconditions;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link NowFilter} represents a filter condition on a single timestamp column which requires
 * the timestamp to be within a certain interval relative to time point at which a query is issued.
 *
 * Because this filter is relative to the query time, it is only possible to execute this filter in the database
 * at query time and hence we try to pull up this filter as much as possible.
 * The only exception are aggregations on relations with a {@link NowFilter} which can be converted to sliding time-window
 * aggregations with some loss of precision (namely the width of the time window).
 *
 * {@link NowFilter} is logically applied before {@link TopNConstraint}.
 *
 */

@Value
public class NowFilter implements PullupOperator {

    public static final NowFilter EMPTY = new NowFilter(List.of());

    private final List<TimePredicate> timePredicates;

    public NowFilter(List<TimePredicate> timePredicates) {
        this.timePredicates = timePredicates;
        timePredicates.forEach(tp -> Preconditions.checkArgument(tp.isNowFilter(),"Not a valid now-filter: %s",tp));
        Set<Integer> indexes = streamTimestampIndexes().collect(Collectors.toSet());
        Preconditions.checkArgument(indexes.size()<=1,"Now filters must be on the same timestamp index: %s",indexes);
    }

    private Stream<Integer> streamTimestampIndexes() {
        return timePredicates.stream().flatMap(tp -> tp.getIndexes().stream()).filter(idx -> idx>=0);
    }

    public int getTimestampIndex() {
        return streamTimestampIndexes().findFirst().get();
    }

    public boolean isEmpty() {
        return timePredicates.isEmpty();
    }


    public NowFilter remap(IndexMap map) {
        return new NowFilter(timePredicates.stream().map(tp -> tp.remap(map)).collect(Collectors.toList()));
    }

    public NowFilter addAll(List<TimePredicate> other) {
        ArrayList<TimePredicate> newList = new ArrayList<>(timePredicates);
        newList.addAll(other);
        return new NowFilter(newList);
    }

}
