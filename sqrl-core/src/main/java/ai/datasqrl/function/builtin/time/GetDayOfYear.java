package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class GetDayOfYear {
    public int getDayOfYear(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getDayOfYear(); }
}
