package ai.dataeng.sqml.ingest.schema.version;

import lombok.Value;

import java.util.List;

public class VersionContainer<C> {

    List<Entry<C>> entries;

    @Value
    private static class Entry<C> {

        private final Version version;
        private final C value;

    }

}
