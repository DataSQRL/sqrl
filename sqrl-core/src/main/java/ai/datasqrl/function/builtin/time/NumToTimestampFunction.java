package ai.datasqrl.function.builtin.time;
import java.time.Instant;

public class NumToTimestampFunction {
    public Instant numToTimestamp(Long l) { return Instant.ofEpochSecond(l); }
}
