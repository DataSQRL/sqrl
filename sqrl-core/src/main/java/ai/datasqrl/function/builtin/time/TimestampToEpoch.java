package ai.datasqrl.function.builtin.time;

import java.time.Instant;

public class TimestampToEpoch {

  public long timestampToEpoch(Instant instant) {
    return instant.toEpochMilli();
  }
}
