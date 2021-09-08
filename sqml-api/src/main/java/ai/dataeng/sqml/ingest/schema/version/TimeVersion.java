package ai.dataeng.sqml.ingest.schema.version;

import java.time.Instant;

public class TimeVersion implements Version<TimeVersion> {

    private final long timepoint;

    public TimeVersion(Instant timepoint) {
        this(timepoint.getEpochSecond());
    }

    public TimeVersion(long secondSinceEpoch) {
        this.timepoint = secondSinceEpoch;
    }

    @Override
    public int compareTo(TimeVersion o) {
        return Long.compare(timepoint, o.timepoint);
    }

    @Override
    public String toString() {
        return "T[" + timepoint + "]";
    }

}
