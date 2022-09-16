package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public class StdTimeLibraryImpl {

  public int getSecond(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond();
  }

  public int getMinute(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute();
  }

  public int getHour(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getHour();
  }

  public int getDayOfWeek(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfWeek().getValue();
  }

  public int getDayOfMonth(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth();
  }

  public int getDayOfYear(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfYear();
  }

  public int getMonth(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue();
  }

  public int getYear(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getYear();
  }

  public Instant roundToSecond(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS)
        .toInstant();
  }

  public Instant roundToMinute(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES)
        .toInstant();
  }

  public Instant roundToHour(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
        .toInstant();
  }

  public Instant roundToDay(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
        .toInstant();
  }

  public Instant roundToMonth(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        .with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS).toInstant();
  }

  public Instant roundToYear(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        .with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant();
  }

  public ZonedDateTime atZone(Instant instant, String zoneId) {
    return instant.atZone(ZoneId.of(zoneId));
  }

  public Instant toUtc(ZonedDateTime zonedDateTime) {
    return zonedDateTime.toInstant();
  }

  public String timestampToString(Instant instant) {
    return instant.toString();
  }

  public Instant stringToTimestamp(String s) {
    return Instant.parse(s);
  }

  public long timestampToEpoch(Instant instant) {
    return instant.toEpochMilli();
  }

  public Instant numToTimestamp(Long l) {
    return Instant.ofEpochSecond(l);
  }

  public Instant now() {
    return Instant.now();
  }
}
