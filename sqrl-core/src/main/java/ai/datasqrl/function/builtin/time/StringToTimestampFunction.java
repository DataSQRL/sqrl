package ai.datasqrl.function.builtin.time;
import org.apache.flink.table.planner.expressions.In;

import java.time.Instant;

public class StringToTimestampFunction {
    public Instant stringToTimestamp(String s) {
        return Instant.parse(s);
    }
}
