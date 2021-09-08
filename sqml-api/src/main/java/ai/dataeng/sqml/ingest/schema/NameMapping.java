package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.schema.version.Version;
import ai.dataeng.sqml.schema2.name.Name;

public interface NameMapping {

    public Name map(Version sourceVersion, Name sourceName);

    public default Name map(Name sourceName) {
        return map(null,sourceName);
    }

}
