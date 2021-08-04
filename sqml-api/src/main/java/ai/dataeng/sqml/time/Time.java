package ai.dataeng.sqml.time;

import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;

public class Time {

    /**
     * TODO: make this configurable
     */
    public static final ZoneId zone = ZoneId.systemDefault();

    public static OffsetDateTime now() {
        return OffsetDateTime.now(ZoneId.systemDefault());
    }

    public static OffsetDateTime convert(FileTime fileTime) {
        return fileTime.toInstant().atZone(zone).toOffsetDateTime();
    }

}
