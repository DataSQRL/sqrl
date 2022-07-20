package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class GetSecond {
    public int getSecond(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getSecond(); }
}
