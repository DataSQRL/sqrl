//DEPS org.apache.flink:flink-table-common:2.1.0

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

@FunctionHint(
    input = {
        @DataTypeHint("DOUBLE"),          // value
        @DataTypeHint("TIMESTAMP_LTZ(3)") // event time (or processing time) as timestamp
    },
    output = @DataTypeHint("DOUBLE")
)
public class __udfname__ extends AggregateFunction<Double, __udfname__.Accumulator> {

  public static class Accumulator {
    public double weightedSum;
    public long totalDuration;
    public double lastValue;
    public long lastTs;
    public boolean initialized;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator();
  }

  /**
   * Accumulate new point.
   *
   * @param acc accumulator
   * @param value current value
   * @param ts timestamp in millis (from TIMESTAMP_LTZ)
   */
  public void accumulate(Accumulator acc, Double value, java.time.Instant ts) {
    if (value == null || ts == null) {
      return; // ignore nulls
    }

    long currentTs = ts.toEpochMilli();

    if (!acc.initialized) {
      // first observation
      acc.initialized = true;
      acc.lastValue = value;
      acc.lastTs = currentTs;
    } else {
      long dt = currentTs - acc.lastTs;
      if (dt > 0) {
        // use previous value over [lastTs, currentTs)
        acc.weightedSum += acc.lastValue * dt;
        acc.totalDuration += dt;
      }
      acc.lastValue = value;
      acc.lastTs = currentTs;
    }
  }

  @Override
  public Double getValue(Accumulator acc) {
    if (!acc.initialized) {
      return null;
    }

    if (acc.totalDuration == 0L) {
      // Only one point so far, fallback to the last value
      return acc.lastValue;
    }

    return acc.weightedSum / (double) acc.totalDuration;
  }

  // Optional merge for batch / grouped aggregation; not used for simple OVER(prefix) but good form.
  public void merge(Accumulator acc, Iterable<Accumulator> it) {
    for (Accumulator other : it) {
      if (!other.initialized) {
        continue;
      }

      if (!acc.initialized) {
        acc.initialized = true;
        acc.lastValue = other.lastValue;
        acc.lastTs = other.lastTs;
        acc.weightedSum = other.weightedSum;
        acc.totalDuration = other.totalDuration;
      } else {
        // We don't know wall-clock relation between segments, so we just stitch sums.
        acc.weightedSum += other.weightedSum;
        acc.totalDuration += other.totalDuration;
        // We could pick some convention for lastValue/lastTs; here we just overwrite.
        acc.lastValue = other.lastValue;
        acc.lastTs = other.lastTs;
      }
    }
  }
}
