package com.datasqrl.util;

import static com.datasqrl.TimeFunctions.END_OF_DAY;
import static com.datasqrl.TimeFunctions.END_OF_HOUR;
import static com.datasqrl.TimeFunctions.END_OF_MINUTE;
import static com.datasqrl.TimeFunctions.END_OF_MONTH;
import static com.datasqrl.TimeFunctions.END_OF_SECOND;
import static com.datasqrl.TimeFunctions.END_OF_WEEK;
import static com.datasqrl.TimeFunctions.END_OF_YEAR;
import static com.datasqrl.TimeFunctions.EPOCH_MILLI_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.EPOCH_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.STRING_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_EPOCH;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_EPOCH_MILLI;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.function.SqrlTimeTumbleFunction;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

public class StdTimeLibraryTest {

  public static final String TIME1_STR = "2023-03-08T18:23:34.083704Z";
  public static Instant TIME1 = t(TIME1_STR);
  public static Instant TIME2 = t("2023-03-12T18:23:34.083Z");
  public static Instant TIME3 = t("2023-03-06T00:23:34.083704Z");

  public static Instant TIME4 = t("2023-01-01T01:23:34.083704Z");

  @Test
  public void testTimeBucket() {
    assertEquals(t("2023-03-08T18:23:34.999999999Z"), END_OF_SECOND.eval(TIME1));
    assertEquals(t("2023-03-08T18:23:39.999999999Z"), END_OF_SECOND.eval(TIME1, 10L));
    assertEquals(t("2023-03-08T18:23:40.199999999Z"), END_OF_SECOND.eval(TIME1, 10L, 200L));
    assertEquals(t("2023-03-08T18:23:34.199999999Z"), END_OF_SECOND.eval(TIME1, 1L, 200L));
    assertEquals(t("2023-03-08T18:23:59.999999999Z"), END_OF_MINUTE.eval(TIME1));
    assertEquals(t("2023-03-08T18:24:59.999999999Z"), END_OF_MINUTE.eval(TIME1, 5L));
    assertEquals(t("2023-03-08T18:25:19.999999999Z"), END_OF_MINUTE.eval(TIME1, 5L, 20L));
    assertEquals(t("2023-03-08T18:59:59.999999999Z"), END_OF_HOUR.eval(TIME1));
    assertEquals(t("2023-03-08T19:59:59.999999999Z"), END_OF_HOUR.eval(TIME1, 4L));
    assertEquals(t("2023-03-08T20:59:59.999999999Z"), END_OF_HOUR.eval(TIME1, 5L));
    assertEquals(t("2023-03-08T20:29:59.999999999Z"), END_OF_HOUR.eval(TIME1, 4L, 30L));
    assertEquals(t("2023-03-08T23:59:59.999999999Z"), END_OF_DAY.eval(TIME1));
    assertEquals(t("2023-03-11T23:59:59.999999999Z"), END_OF_DAY.eval(TIME1, 10L));
    assertEquals(t("2023-03-09T02:59:59.999999999Z"), END_OF_DAY.eval(TIME1, 1L, 3L));
    assertEquals(t("2023-01-01T01:59:59.999999999Z"), END_OF_DAY.eval(TIME4, 10L, 2L));
    assertEquals(t("2023-03-12T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME1));
    assertEquals(t("2023-03-12T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME2));
    assertEquals(t("2023-03-12T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME3));
    assertEquals(t("2023-03-13T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME1, 1L, 1L));
    assertEquals(t("2023-03-08T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME1, 1L, 3L));
    assertEquals(t("2023-03-31T23:59:59.999999999Z"), END_OF_MONTH.eval(TIME1));
    assertEquals(t("2023-04-05T23:59:59.999999999Z"), END_OF_MONTH.eval(TIME1, 1L, 5L));
    assertEquals(t("2023-03-10T23:59:59.999999999Z"), END_OF_MONTH.eval(TIME1, 1L, 10L));
    assertEquals(t("2023-12-31T23:59:59.999999999Z"), END_OF_YEAR.eval(TIME1));
    assertEquals(t("2024-12-31T23:59:59.999999999Z"), END_OF_YEAR.eval(TIME1, 5L));
    assertEquals(t("2030-01-20T23:59:59.999999999Z"), END_OF_YEAR.eval(TIME1, 10L, 20L));
  }

  @Test
  public void testTimeWindowSpec() {
    assertTimeWindowSpec(END_OF_SECOND.getSpecification(new long[]{3, 200}), 3000, 200);
    assertTimeWindowSpec(END_OF_MINUTE.getSpecification(new long[]{3, 20}), 3*60*1000, 20*1000);
    assertTimeWindowSpec(END_OF_HOUR.getSpecification(new long[]{3, 10}), 3*3600*1000, 10*60*1000);
    assertTimeWindowSpec(END_OF_DAY.getSpecification(new long[]{3, 2}), 3*24*3600*1000, 2*3600*1000);
    assertTimeWindowSpec(END_OF_WEEK.getSpecification(new long[]{1, 2}), 7*24*3600*1000, 2*24*3600*1000);
    assertTimeWindowSpec(END_OF_MONTH.getSpecification(new long[]{1, 7}), 2629746000L, 7*24*3600*1000);
    assertTimeWindowSpec(END_OF_YEAR.getSpecification(new long[]{2, 20}), 2*31556952000L, 20L*24*3600*1000);
  }

  public static void assertTimeWindowSpec(SqrlTimeTumbleFunction.Specification spec, long width, long offset) {
    assertEquals(width, spec.getWindowWidthMillis());
    assertEquals(offset, spec.getWindowOffsetMillis());
  }

  @Test
  public void testTimeConversion() {
    assertEquals(TIME2, EPOCH_MILLI_TO_TIMESTAMP.eval(TIME2.toEpochMilli()));
    assertEquals(TIME1.truncatedTo(ChronoUnit.SECONDS), EPOCH_TO_TIMESTAMP.eval(TIME1.toEpochMilli()/1000));
    assertEquals(TIME3.truncatedTo(ChronoUnit.SECONDS), EPOCH_TO_TIMESTAMP.eval(TIME3.toEpochMilli()/1000));
    assertEquals(TIME1.toEpochMilli()/1000, TIMESTAMP_TO_EPOCH.eval(TIME1));
    assertEquals(TIME1.toEpochMilli(), TIMESTAMP_TO_EPOCH_MILLI.eval(TIME1));
    assertEquals(TIME1, STRING_TO_TIMESTAMP.eval(TIME1_STR));
    assertEquals(TIME1_STR, TIMESTAMP_TO_STRING.eval(TIME1));
    assertEquals(t("2023-09-27T06:45:00Z"), STRING_TO_TIMESTAMP.eval("September 26, 2023 11:45 PM PDT", "MMMM dd, yyyy hh:mm a z"));
    assertEquals(t("2023-09-27T21:30:00Z"), STRING_TO_TIMESTAMP.eval("September 27, 2023 2:30 PM PDT", "MMMM d, yyyy h:mm a z"));
//    assertEquals(t("2023-03-08T13:23:34.083704-05:00"), AT_ZONE.eval(TIME1, "GMT-8"));
  }



  @Test
  public void convert() {
    System.out.println(Instant.parse("2023-04-06T03:46:25.260230Z").toEpochMilli());
    System.out.println(Instant.parse("2023-02-27T21:35:13.903106Z").toEpochMilli());
    System.out.println(Instant.parse("2023-03-14T22:53:24.579297Z").toEpochMilli());
    System.out.println(Instant.parse("2023-04-06T03:50:51.034470Z").toEpochMilli());
  }



  public static Instant t(String timestamp) {
    return Instant.parse(timestamp);
  }

}
