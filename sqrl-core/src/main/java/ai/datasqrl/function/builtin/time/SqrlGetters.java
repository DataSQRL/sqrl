package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class SqrlGetters {
    public int getSecond(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getSecond(); }
    public int getMinute(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getMinute(); }
    public int getHour(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getHour(); }
    public int getDayOfWeek(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getDayOfWeek().getValue(); }
    public int getDayOfMonth(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getDayOfMonth(); }
    public int getDayOfYear(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getDayOfYear(); }
    public int getMonth(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getMonthValue(); }
    public int getYear(Instant instant) { return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).getYear(); }
}
