package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class GetMonth {
    public int getMonth(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getMonthValue(); }
}
