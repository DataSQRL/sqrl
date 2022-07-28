package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public class SqrlRounding {

  public Instant roundToSecond(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).truncatedTo(ChronoUnit.SECONDS)
        .toInstant();
  }

  public Instant roundToMinute(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).truncatedTo(ChronoUnit.MINUTES)
        .toInstant();
  }

  public Instant roundToHour(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).truncatedTo(ChronoUnit.HOURS)
        .toInstant();
  }

  public Instant roundToDay(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).truncatedTo(ChronoUnit.DAYS)
        .toInstant();
  }

  public Instant roundToMonth(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
        .with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS).toInstant();
  }

  public Instant roundToYear(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
        .with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant();
  }
}
