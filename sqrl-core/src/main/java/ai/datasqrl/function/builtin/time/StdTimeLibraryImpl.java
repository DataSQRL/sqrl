package ai.datasqrl.function.builtin.time;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import lombok.Value;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

public class StdTimeLibraryImpl {

  public static final List<FlinkFnc> fncs = List.of(
      new FlinkFnc(NOW.class.getSimpleName(), new NOW()),
      new FlinkFnc(EPOCH_TO_TIMESTAMP.class.getSimpleName(), new EPOCH_TO_TIMESTAMP()),
      new FlinkFnc(TIMESTAMP_TO_EPOCH.class.getSimpleName(), new TIMESTAMP_TO_EPOCH()),
      new FlinkFnc(STRING_TO_TIMESTAMP.class.getSimpleName(), new STRING_TO_TIMESTAMP()),
      new FlinkFnc(TIMESTAMP_TO_STRING.class.getSimpleName(), new TIMESTAMP_TO_STRING()),
      new FlinkFnc(TO_UTC.class.getSimpleName(), new TO_UTC()),
      new FlinkFnc(AT_ZONE.class.getSimpleName(), new AT_ZONE()),
      new FlinkFnc(ROUND_TO_SECOND.class.getSimpleName(), new ROUND_TO_SECOND()),
      new FlinkFnc(ROUND_TO_MINUTE.class.getSimpleName(), new ROUND_TO_MINUTE()),
      new FlinkFnc(ROUND_TO_HOUR.class.getSimpleName(), new ROUND_TO_HOUR()),
      new FlinkFnc(ROUND_TO_DAY.class.getSimpleName(), new ROUND_TO_DAY()),
      new FlinkFnc(ROUND_TO_MONTH.class.getSimpleName(), new ROUND_TO_MONTH()),
      new FlinkFnc(ROUND_TO_YEAR.class.getSimpleName(), new ROUND_TO_YEAR()),
      new FlinkFnc(GET_SECOND.class.getSimpleName(), new GET_SECOND()),
      new FlinkFnc(GET_MINUTE.class.getSimpleName(), new GET_MINUTE()),
      new FlinkFnc(GET_HOUR.class.getSimpleName(), new GET_HOUR()),
      new FlinkFnc(GET_DAY_OF_WEEK.class.getSimpleName(), new GET_DAY_OF_WEEK()),
      new FlinkFnc(GET_DAY_OF_MONTH.class.getSimpleName(), new GET_DAY_OF_MONTH()),
      new FlinkFnc(GET_DAY_OF_YEAR.class.getSimpleName(), new GET_DAY_OF_YEAR()),
      new FlinkFnc(GET_MONTH.class.getSimpleName(), new GET_DAY_OF_YEAR()),
      new FlinkFnc(GET_YEAR.class.getSimpleName(), new GET_YEAR())
  );

  public static class GET_SECOND extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getSecond();
    }
  }

  public static class GET_MINUTE extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMinute();
    }
  }

  public static class GET_HOUR extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getHour();
    }

  }

  public static class GET_DAY_OF_WEEK extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfWeek().getValue();
    }
  }

  public static class GET_DAY_OF_MONTH extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfMonth();
    }
  }

  public static class GET_DAY_OF_YEAR extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getDayOfYear();
    }

  }

  public static class GET_MONTH extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getMonthValue();
    }
  }

  public static class GET_YEAR extends ScalarFunction {

    public int eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).getYear();
    }
  }

  public static class ROUND_TO_SECOND extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS)
          .toInstant();
    }
  }

  public static class ROUND_TO_MINUTE extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES)
          .toInstant();
    }
  }

  public static class ROUND_TO_HOUR extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
          .toInstant();
    }
  }

  public static class ROUND_TO_DAY extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
          .toInstant();
    }
  }

  public static class ROUND_TO_MONTH extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }
  }

  public static class ROUND_TO_YEAR extends ScalarFunction {

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant();
    }
  }

  public static class AT_ZONE extends ScalarFunction {

    public ZonedDateTime eval(Instant instant, String zoneId) {
      return instant.atZone(ZoneId.of(zoneId));
    }
  }

  public static class TO_UTC extends ScalarFunction {

    public Instant eval(ZonedDateTime zonedDateTime) {
      return zonedDateTime.toInstant();
    }
  }

  public static class TIMESTAMP_TO_STRING extends ScalarFunction {

    public String eval(Instant instant) {
      return instant.toString();
    }
  }

  public static class STRING_TO_TIMESTAMP extends ScalarFunction {

    public Instant eval(String s) {
      return Instant.parse(s);
    }
  }

  public static class TIMESTAMP_TO_EPOCH extends ScalarFunction {

    public Long eval(Instant instant) {
      return instant.toEpochMilli();
    }
  }

  public static class EPOCH_TO_TIMESTAMP extends ScalarFunction {
    public Instant eval(Long l) {
      return Instant.ofEpochSecond(l.longValue());
    }

  }

  public static class NOW extends ScalarFunction {

    public Instant eval() {
      return Instant.now();
    }
  }

  @Value
  public static class FlinkFnc {

    String name;
    UserDefinedFunction fnc;
  }
}
