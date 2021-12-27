package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.Column;
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

        final Column column;
        final Direction direction;

    }

    public enum Direction {
        ASC, DESC;
    }


}
