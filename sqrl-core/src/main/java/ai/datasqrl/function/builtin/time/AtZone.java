package ai.datasqrl.function.builtin.time;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class AtZone {

  public ZonedDateTime atZone(Instant instant, String zoneId) {
    return instant.atZone(ZoneId.of(zoneId));
  }
}
