package ai.datasqrl.function.builtin.time;

import java.time.Instant;

public class StringToTimestamp {

  public Instant stringToTimestamp(String s) {
    return Instant.parse(s);
  }
}
