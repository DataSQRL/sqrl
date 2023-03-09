package com.datasqrl.util;

import static com.datasqrl.function.builtin.time.StdTimeLibraryImpl.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

public class StdTimeLibraryTest {

  public static final String TIME1_STR = "2023-03-08T18:23:34.083704Z";
  public static Instant TIME1 = t(TIME1_STR);
  public static Instant TIME2 = t("2023-03-12T18:23:34.083Z");
  public static Instant TIME3 = t("2023-03-06T00:23:34.083704Z");

  @Test
  public void testTimeBucket() {
    assertEquals(t("2023-03-08T18:23:34.999999999Z"), END_OF_SECOND.eval(TIME1));
    assertEquals(t("2023-03-08T18:23:59.999999999Z"), END_OF_MINUTE.eval(TIME1));
    assertEquals(t("2023-03-08T18:59:59.999999999Z"), END_OF_HOUR.eval(TIME1));
    assertEquals(t("2023-03-08T23:59:59.999999999Z"), END_OF_DAY.eval(TIME1));
    assertEquals(t("2023-03-12T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME1));
    assertEquals(t("2023-03-12T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME2));
    assertEquals(t("2023-03-12T23:59:59.999999999Z"), END_OF_WEEK.eval(TIME3));
    assertEquals(t("2023-03-31T23:59:59.999999999Z"), END_OF_MONTH.eval(TIME1));
    assertEquals(t("2023-12-31T23:59:59.999999999Z"), END_OF_YEAR.eval(TIME1));
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

//    assertEquals(t("2023-03-08T13:23:34.083704-05:00"), AT_ZONE.eval(TIME1, "GMT-8"));
  }




  public static Instant t(String timestamp) {
    return Instant.parse(timestamp);
  }

}
