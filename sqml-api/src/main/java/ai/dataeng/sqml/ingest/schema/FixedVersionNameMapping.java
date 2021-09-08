package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.schema.version.Version;
import ai.dataeng.sqml.schema2.name.Name;
import com.google.common.base.Preconditions;

import java.util.Map;

public class FixedVersionNameMapping implements NameMapping {

    private final Map<Name, Name> mapping;
    private final Version fixedVersion;

    public FixedVersionNameMapping(Map<Name, Name> mapping, Version fixedVersion) {
        this.mapping = mapping;
        this.fixedVersion = fixedVersion;
    }

    @Override
    public Name map(Version sourceVersion, Name sourceName) {
        if (fixedVersion!=null) Preconditions.checkArgument(sourceVersion.equals(fixedVersion),
                "Incompatible versions: [%s] vs [%s]", fixedVersion, sourceVersion);
        Name result = mapping.get(sourceName);
        if (result==null) return sourceName;
        else return result;
    }
}
