package ai.datasqrl.plan.calcite.util;

import lombok.Value;

/**
 * Represents a timestamp predicate that is normalized into the form:
 * left_timestamp <=/</= right_timestamp + interval
 * The index values refer to timestamp columns in a relation by index or a special timestamp designator.
 */
@Value
public class TimePredicate {

    public static final int NOW_INDEX = -10;

    final int leftIndex;
    final int rightIndex;
    final Comparison comparison;
    final Object interval;

    public enum Comparison {
        LESS, LESS_EQ, EQUAL;
    }

}
