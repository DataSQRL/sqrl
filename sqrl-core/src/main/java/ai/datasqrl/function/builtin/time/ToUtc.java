package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZonedDateTime;

public class ToUtc {

  public Instant toUtc(ZonedDateTime zonedDateTime) {
    return zonedDateTime.toInstant();
  }

//    public Instant toUtc(OffsetDateTime offsetDateTime) {
//        return offsetDateTime.toInstant();
//    }
}
