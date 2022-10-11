package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import ai.datasqrl.plan.calcite.util.TimePredicate;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

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


public interface NowFilter extends PullupOperator {

    public static final NowFilter EMPTY = new NowFilter(){};

    default TimePredicate getPredicate() {
        throw new IllegalStateException();
    }

    default int getTimestampIndex() {
        return getPredicate().getLargerIndex();
    }

    default boolean isEmpty() {
        return true;
    }

    default NowFilter remap(IndexMap map) {
        return this;
    }

    default NowFilter map(Function<TimePredicate,TimePredicate> mapping) {
        return this;
    }

    default RelBuilder addFilterTo(RelBuilder relBuilder) {
        return addFilterTo(relBuilder,false);
    }

    default RelBuilder addFilterTo(RelBuilder relBuilder, boolean useCurrentTime) {
        return relBuilder;
    }

    static NowFilter of(TimePredicate nowPredicate) {
        return new NowFilterImpl(nowPredicate);
    }

    static Optional<NowFilter> of(List<TimePredicate> timePreds) {
        if (timePreds.isEmpty()) return Optional.of(EMPTY);
        Optional<TimePredicate> combined = Optional.of(timePreds.get(0));
        for (int i = 1; i < timePreds.size(); i++) {
            combined = combined.flatMap(timePreds.get(i)::and);
        }
        return combined.map(NowFilterImpl::new);
    }

    default Optional<NowFilter> merge(NowFilter other) {
        if (isEmpty()) return Optional.of(other);
        if (other.isEmpty()) return Optional.of(this);
        return of(List.of(getPredicate(),other.getPredicate()));
    }


    @Value
    public static class NowFilterImpl implements NowFilter {

        private final TimePredicate nowPredicate;

        public NowFilterImpl(TimePredicate nowPredicate) {
            Preconditions.checkArgument(nowPredicate.isNowPredicate());
            this.nowPredicate = nowPredicate;
        }

        @Override
        public TimePredicate getPredicate() {
            return nowPredicate;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public NowFilter remap(IndexMap map) {
            return new NowFilterImpl(nowPredicate.remap(map));
        }

        @Override
        public NowFilter map(Function<TimePredicate,TimePredicate> mapping) {
            return new NowFilterImpl(mapping.apply(nowPredicate));
        }

        @Override
        public RelBuilder addFilterTo(RelBuilder relBuilder, boolean useCurrentTime) {
            RexBuilder rexB = relBuilder.getRexBuilder();
            relBuilder.filter(getPredicate().createRexNode(rexB,i -> rexB.makeInputRef(relBuilder.peek(),i), useCurrentTime));
            return relBuilder;
        }
    }

}
