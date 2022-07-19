package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public class RoundToYear {
    public Instant roundToYear(Instant instant) {
        return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }
}
