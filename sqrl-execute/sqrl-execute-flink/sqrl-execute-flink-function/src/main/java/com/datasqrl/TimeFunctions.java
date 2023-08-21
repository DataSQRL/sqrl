package com.datasqrl;

import com.datasqrl.SqrlFunctions.VariableArguments;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Locale;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

public class TimeFunctions {
  public static final EpochToTimestamp EPOCH_TO_TIMESTAMP = new EpochToTimestamp();
  public static final EpochMilliToTimestamp EPOCH_MILLI_TO_TIMESTAMP = new EpochMilliToTimestamp();
  public static final TimestampToEpoch TIMESTAMP_TO_EPOCH = new TimestampToEpoch();
  public static final TimestampToEpochMilli TIMESTAMP_TO_EPOCH_MILLI = new TimestampToEpochMilli();
  public static final ParseTimestamp STRING_TO_TIMESTAMP = new ParseTimestamp();
  public static final TimestampToString TIMESTAMP_TO_STRING = new TimestampToString();
  public static final AtZone AT_ZONE = new AtZone();
  public static final EndOfSecond END_OF_SECOND = new EndOfSecond();
  public static final EndOfMinute END_OF_MINUTE = new EndOfMinute();
  public static final EndOfHour END_OF_HOUR = new EndOfHour();
  public static final EndOfDay END_OF_DAY = new EndOfDay();
  public static final EndOfWeek END_OF_WEEK = new EndOfWeek();
  public static final EndOfMonth END_OF_MONTH = new EndOfMonth();
  public static final EndOfYear END_OF_YEAR = new EndOfYear();
  private static final Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");



  public static class EndOfSeconds extends ScalarFunction implements SqrlFunction,
      SqrlTimeTumbleFunction {

    @Override
    public String getDocumentation() {
      return "Rounds timestamp to the end of the given interval and offset.";
    }

    public Instant eval(Instant instant, Integer width, Integer offset) {
      throw new NotYetImplementedException("testing");
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .inputTypeStrategy(VariableArguments.builder()
              .staticType(DataTypes.INT())
              .variableType(DataTypes.INT())
              .minVariableArguments(0)
              .maxVariableArguments(1)
              .build())
          .outputTypeStrategy(
              SqrlFunctions.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
          .build();
    }

    @Override
    public Specification getSpecification(long[] arguments) {
      Preconditions.checkArgument(arguments.length == 1 || arguments.length == 2);
      return new Specification() {
        @Override
        public long getWindowWidthMillis() {
          return arguments[0]*1000;
        }

        @Override
        public long getWindowOffsetMillis() {
          return arguments.length>1?arguments[1]*1000:0;
        }
      };
    }

  }

  private interface TimeWindowBucketFunctionEval {

    Instant eval(Instant instant, Long multiple, Long offset);

    default Instant eval(Instant instant, Long multiple) {
      return eval(instant, multiple, 0l);
    }

    default Instant eval(Instant instant) {
      return eval(instant, 1l);
    }

  }

  @AllArgsConstructor
  public abstract static class TimeWindowBucketFunction extends ScalarFunction implements SqrlFunction,
      SqrlTimeTumbleFunction, TimeWindowBucketFunctionEval {

    protected final ChronoUnit timeUnit;
    protected final ChronoUnit offsetUnit;

    @Override
    public String getDocumentation() {
      Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");
      String functionCall = String.format("%s(%s(%s))",
          getFunctionName().getDisplay(),
          STRING_TO_TIMESTAMP.getFunctionName().getDisplay(),
          DEFAULT_DOC_TIMESTAMP.toString());
      String result = this.eval(DEFAULT_DOC_TIMESTAMP).toString();

      String timeUnitName = timeUnit.toString().toLowerCase();
      String timeUnitNameSingular = StringUtil.removeFromEnd(timeUnitName,"s");
      String offsetUnitName = offsetUnit.toString().toLowerCase();
      return String.format("Time window function that returns the end of %s for the timestamp argument."
              + "<br />E.g. `%s` returns the timestamp `%s`."
              + "<br />An optional second argument specifies the time window width as multiple %s, e.g. if the argument is 5 the function returns the end of the next 5 %s. 1 is the default."
              + "<br />An optional third argument specifies the time window offset in %s, e.g. if the argument is 2 the function returns the end of the time window offset by 2 %s",
          timeUnitNameSingular,
          functionCall, result,
          timeUnitName, timeUnitName,
          offsetUnitName, offsetUnitName);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .inputTypeStrategy(VariableArguments.builder()
              .staticType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
              .variableType(DataTypes.BIGINT())
              .minVariableArguments(0)
              .maxVariableArguments(2)
              .build())
          .outputTypeStrategy(
              SqrlFunctions.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
          .build();
    }

    @Override
    public Specification getSpecification(long[] arguments) {
      Preconditions.checkArgument(arguments!= null);
      return new Specification(arguments.length>0?arguments[0]:1,
          arguments.length>1?arguments[1]:0);
    }

    @AllArgsConstructor
    private class Specification implements SqrlTimeTumbleFunction.Specification {

      final long widthMultiple;
      final long offsetMultiple;

      @Override
      public long getWindowWidthMillis() {
        return timeUnit.getDuration().multipliedBy(widthMultiple).toMillis();
      }

      @Override
      public long getWindowOffsetMillis() {
        return offsetUnit.getDuration().multipliedBy(offsetMultiple).toMillis();
      }
    }

    @Override
    public Instant eval(Instant instant, Long multiple, Long offset) {
      if (multiple==null) multiple=1l;
      Preconditions.checkArgument(multiple>0, "Window width must be positive: %s", multiple);
      if (offset==null) offset=0l;
      Preconditions.checkArgument(offset>=0, "Invalid window offset: %s", offset);
      Preconditions.checkArgument(offsetUnit.getDuration().multipliedBy(offset).compareTo(timeUnit.getDuration())<0,
          "Offset of %s %s is larger than %s", offset, offsetUnit, timeUnit);

      ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
      ZonedDateTime truncated = time.minus(offset, offsetUnit).truncatedTo(timeUnit);

      long multipleToAdd = 1;
      if (multiple>1) {
        ZonedDateTime truncatedBase = truncated.with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS);
        ZonedDateTime timeBase = time.with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS);
        if (!timeBase.equals(truncatedBase)) {
          //We slipped into the prior base unit (i.e. year) due to offset.
          return timeBase.plus(offset, offsetUnit).minus(1, ChronoUnit.NANOS)
              .toInstant();
        }
        Duration timeToBase = Duration.between(truncatedBase, truncated);
        long numberToBase = timeToBase.dividedBy(timeUnit.getDuration());
        multipleToAdd = multiple - (numberToBase % multiple);
      }

      return truncated.plus(multipleToAdd, timeUnit).plus(offset, offsetUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

  }

  public static class EndOfSecond extends TimeWindowBucketFunction {

    public EndOfSecond() {
      super(ChronoUnit.SECONDS, ChronoUnit.MILLIS);
    }


  }

  public static class EndOfMinute extends TimeWindowBucketFunction {

    public EndOfMinute() {
      super(ChronoUnit.MINUTES, ChronoUnit.SECONDS);
    }

  }

  public static class EndOfHour extends TimeWindowBucketFunction {

    public EndOfHour() {
      super(ChronoUnit.HOURS, ChronoUnit.MINUTES);
    }

  }

  public static class EndOfDay extends TimeWindowBucketFunction {

    public EndOfDay() {
      super(ChronoUnit.DAYS, ChronoUnit.HOURS);
    }


  }

  public static class EndOfWeek extends TimeWindowBucketFunction {

    public EndOfWeek() {
      super(ChronoUnit.WEEKS, ChronoUnit.DAYS);
    }

    @Override
    public Instant eval(Instant instant, Long multiple, Long offset) {
      if (multiple==null) multiple=1l;
      Preconditions.checkArgument(multiple==1, "Time window width must be 1. Use endofDay instead for flexible window widths.");
      if (offset==null) offset=0l;
      Preconditions.checkArgument(offset>=0 && offset<=6, "Invalid offset in days: %s", offset);

      ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
      int daysToSubtract = time.getDayOfWeek().getValue()-1-offset.intValue();
      if (daysToSubtract<0) daysToSubtract = 7+daysToSubtract;
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
          .minus(daysToSubtract, ChronoUnit.DAYS)
          .plus(1,timeUnit).minus(1, ChronoUnit.NANOS)
          .toInstant();
    }


  }

  public static class EndOfMonth extends TimeWindowBucketFunction {

    public EndOfMonth() {
      super(ChronoUnit.MONTHS, ChronoUnit.DAYS);
    }

    public Instant eval(Instant instant, Long multiple, Long offset) {
      if (multiple==null) multiple=1l;
      Preconditions.checkArgument(multiple==1, "Time window width must be 1. Use endofDay instead for flexible window widths.");
      if (offset==null) offset=0l;
      Preconditions.checkArgument(offset>=0 && offset<=28, "Invalid offset in days: %s", offset);

      ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
      if (time.getDayOfMonth() > offset) time =  time.with(TemporalAdjusters.firstDayOfNextMonth());
      else time = time.with(TemporalAdjusters.firstDayOfMonth());
      time = time.plus(offset, ChronoUnit.DAYS);
      return time.minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

  }

  public static class EndOfYear extends TimeWindowBucketFunction {

    public EndOfYear() {
      super(ChronoUnit.YEARS, ChronoUnit.DAYS);
    }

    public Instant eval(Instant instant) {
      return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
          .with(TemporalAdjusters.firstDayOfNextYear()).truncatedTo(ChronoUnit.DAYS)
          .minus(1, ChronoUnit.NANOS)
          .toInstant();
    }

    public Instant eval(Instant instant, Long multiple, Long offset) {
      if (multiple==null) multiple=1l;
      Preconditions.checkArgument(multiple>0 && multiple<Integer.MAX_VALUE, "Window width must be a positive integer value: %s", multiple);
      if (offset==null) offset=0l;
      Preconditions.checkArgument(offset>=0 && offset<365, "Invalid offset in days: %s", offset);

      ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
      if (time.getDayOfYear() > offset) time =  time.with(TemporalAdjusters.firstDayOfNextYear());
      else time = time.with(TemporalAdjusters.firstDayOfYear());
      int modulus = multiple.intValue();
      int yearsToAdd = (modulus - time.getYear()%modulus)%modulus;

      time = time.plus(yearsToAdd, ChronoUnit.YEARS).plus(offset, ChronoUnit.DAYS);
      return time.minus(1, ChronoUnit.NANOS)
          .toInstant();
    }


  }

  public static class AtZone extends ScalarFunction implements SqrlFunction,
      TimestampPreservingFunction {

    public ZonedDateTime eval(Instant instant, String zoneId) {
      return instant.atZone(ZoneId.of(zoneId));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.STRING())
          .outputTypeStrategy(callContext -> {
            DataType type = SqrlFunctions.getFirstArgumentType(callContext);
            if (type.getLogicalType().isNullable()) {
              return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
            }

            return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
          })
          .build();
    }


    @Override
    public String getDocumentation() {
      return String.format("Returns the timestamp at the given timezone.");
    }
  }

  public static class TimestampToString extends ScalarFunction implements SqrlFunction {

    public String eval(Instant instant) {
      return instant.toString();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return SqrlFunctions.basicNullInference(DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }


    @Override
    public String getDocumentation() {
      return String.format("Converts the timestamp to an ISO timestamp string");
    }
  }

  public static class ParseTimestamp extends ScalarFunction implements SqrlFunction {

    public Instant eval(String s) {
      return Instant.parse(s);
    }

    public Instant eval(String s, String format) {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format, Locale.US);
      return LocalDateTime.parse(s, formatter)
          .atZone(ZoneId.systemDefault())
          .toInstant();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .inputTypeStrategy(VariableArguments.builder()
              .staticType(DataTypes.STRING())
              .variableType(DataTypes.STRING())
              .minVariableArguments(0)
              .maxVariableArguments(1)
              .build())
          .outputTypeStrategy(
              SqrlFunctions.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
          .build();
    }


    @Override
    public String getDocumentation() {
      return String.format("Parses a timestamp from an ISO timestamp string.");
    }
  }



  @AllArgsConstructor
  private abstract static class AbstractTimestampToEpoch extends ScalarFunction implements SqrlFunction {

    private final boolean isMilli;

    public Long eval(Instant instant) {
      long epoch = instant.toEpochMilli();
      if (!isMilli) epoch = epoch/1000;
      return epoch;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return SqrlFunctions.basicNullInference(DataTypes.BIGINT(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

    @Override
    public String getDocumentation() {
      Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");
      String functionCall = String.format("%s(%s(%s))",
          getFunctionName().getDisplay(),
          STRING_TO_TIMESTAMP.getFunctionName().getDisplay(),
          DEFAULT_DOC_TIMESTAMP.toString());
      String result = this.eval(DEFAULT_DOC_TIMESTAMP).toString();
      return String.format("Returns the %s since epoch for the given timestamp.<br />E.g. `%s` returns the number `%s`",
          isMilli?"milliseconds":"seconds",
          functionCall, result);
    }

  }

  public static class TimestampToEpoch extends AbstractTimestampToEpoch {

    public TimestampToEpoch() {
      super(false);
    }
  }

  public static class TimestampToEpochMilli extends AbstractTimestampToEpoch {

    public TimestampToEpochMilli() {
      super(true);
    }
  }


  @AllArgsConstructor
  public static class AbstractEpochToTimestamp extends ScalarFunction implements SqrlFunction {

    boolean isMilli;


    public Instant eval(Long l) {
      if (isMilli) return Instant.ofEpochMilli(l.longValue());
      else return Instant.ofEpochSecond(l.longValue());
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return SqrlFunctions.basicNullInference(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.BIGINT());
    }

    @Override
    public String getDocumentation() {
      Instant inst = DEFAULT_DOC_TIMESTAMP.truncatedTo(ChronoUnit.SECONDS);
      long epoch = inst.toEpochMilli()/(isMilli?1:1000);
      String functionCall = String.format("%s(%s)",
          getFunctionName().getDisplay(), epoch);
      String result = this.eval(epoch).toString();
      return String.format("Converts the epoch timestamp in %s to the corresponding timestamp.<br />E.g. `%s` returns the timestamp `%s`",
          isMilli?"milliseconds":"seconds",
          functionCall, result);
    }

  }

  public static class EpochToTimestamp extends AbstractEpochToTimestamp {

    public EpochToTimestamp() {
      super(false);
    }

  }

  public static class EpochMilliToTimestamp extends AbstractEpochToTimestamp {

    public EpochMilliToTimestamp() {
      super(true);
    }
  }



}
