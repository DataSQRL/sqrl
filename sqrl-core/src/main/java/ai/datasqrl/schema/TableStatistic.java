package ai.datasqrl.schema;

import ai.datasqrl.io.sources.stats.RelationStats;
import lombok.Value;

@Value
public class TableStatistic {

    private final double rowCount;

    public static TableStatistic from(RelationStats relationStats) {
        return new TableStatistic(relationStats.getCount());
    }

    public static TableStatistic of(double rowCount) {
        return new TableStatistic(rowCount);
    }

}
