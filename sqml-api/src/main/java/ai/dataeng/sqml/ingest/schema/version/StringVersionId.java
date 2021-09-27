package ai.dataeng.sqml.ingest.schema.version;

import lombok.NonNull;
import lombok.Value;

@Value
public class StringVersionId implements VersionIdentifier {

    private final String id;

    public static StringVersionId of(@NonNull String id) {
        return new StringVersionId(id.trim());
    }

    @Override
    public String toString() {
        return id;
    }


}
