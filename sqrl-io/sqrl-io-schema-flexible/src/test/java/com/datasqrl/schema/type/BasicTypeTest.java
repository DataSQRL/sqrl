package com.datasqrl.schema.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.schema.type.basic.DateTimeType;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BasicTypeTest {

  @Test
  @Disabled
  public void testDateTimeParsing() {
    String[] timeStrs =    {"2022-07-15 10:15:30" , "2022-07-15 10:15:30.543", "2022-07-15T10:15:30Z", "2011-12-03T10:15:30+01:00", "2011-12-03T10:15:30"};
    String[] resultTimes = {"2022-07-15T17:15:30Z","2022-07-15T17:15:30.543Z", "2022-07-15T10:15:30Z", "2011-12-03T09:15:30Z",      "2011-12-03T18:15:30Z"};
    for (int i = 0; i < timeStrs.length; i++) {
      String timeStr = timeStrs[i];
      assertTrue(DateTimeType.INSTANCE.conversion().detectType(timeStr),timeStr);
      ErrorCollector errors = ErrorCollector.root();
      Optional<Instant> result = DateTimeType.INSTANCE.conversion().parseDetected(timeStr, errors);
      assertTrue(result.isPresent());
      assertNotNull(result.get());
      assertEquals(Instant.parse(resultTimes[i]),result.get());
//      System.out.println(result.get());
      assertTrue(errors.isEmpty());
    }
  }


}
