package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import lombok.Value;

@Value
public class TableStatistic {

    private static final double DEFAULT_ROW_COUNT = 1e15;

    public static final TableStatistic UNKNOWN = new TableStatistic(Double.NaN);

    private final double rowCount;

    public static TableStatistic from(SourceTableStatistics tableStatistics) {
        if (tableStatistics.getCount()<=0) return UNKNOWN;
        return new TableStatistic(tableStatistics.getCount());
    }

    public static TableStatistic of(double rowCount) {
        return new TableStatistic(rowCount);
    }

    @Override
    public String toString() {
        return "Stats="+rowCount;
    }

    public boolean isUnknown() {
        return Double.isNaN(rowCount);
    }

    public double getRowCount() {
        if (isUnknown()) return DEFAULT_ROW_COUNT;
        else return rowCount;
    }

}
