package ai.datasqrl.function.builtin.time;

import java.time.*;

public class AtZone {
    public ZonedDateTime atZone(Instant instant, String zoneId) {
        return instant.atZone(ZoneId.of(zoneId));
    }
//    public OffsetDateTime atZone(Instant instant, String offsetId) {
//        return instant.atOffset(ZoneOffset.of(offsetId));
//    }
}
