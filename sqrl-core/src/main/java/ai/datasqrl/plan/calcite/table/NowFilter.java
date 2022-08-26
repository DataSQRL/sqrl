package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import lombok.Value;

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

public interface NowFilter extends DatabasePullup {

    NowFilter EMPTY = new NowFilter() {

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public NowFilter remap(IndexMap map) {
            return EMPTY;
        }

        @Override
        public int getColumnIndex() {
            throw new UnsupportedOperationException();
        }

    };

    default boolean isEmpty() {
        return false;
    }

    NowFilter remap(IndexMap map);

    int getColumnIndex();

    @Value
    public class Impl implements NowFilter {

        private final int columnIndex;
        //private final Interval interval;

        public NowFilter remap(IndexMap map) {
            return new NowFilter.Impl(map.map(columnIndex));
        }

    }

}
