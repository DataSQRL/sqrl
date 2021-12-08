package ai.dataeng.sqml.logical4;

import lombok.Value;

import java.util.List;

/**
 * Represents an ordering of rows
 */
@Value
public class Order {

    final List<Entry> entries;

    @Value
    public static class Entry {

        final LogicalPlan.Column column;
        final Direction direction;

    }

    public enum Direction {
        ASC, DESC;
    }


}
