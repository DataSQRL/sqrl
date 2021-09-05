package ai.dataeng.sqml.ingest.schema.version;

import java.time.Instant;

public class TimeVersion implements Version<TimeVersion> {

    private final long timepoint;

    public TimeVersion(Instant timepoint) {
        this.timepoint = timepoint.getEpochSecond();
    }

    @Override
    public int compareTo(TimeVersion o) {
        return Long.compare(timepoint, o.timepoint);
    }

}
