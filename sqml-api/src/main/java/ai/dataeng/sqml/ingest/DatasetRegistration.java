package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.schema.version.Version;
import ai.dataeng.sqml.ingest.schema.version.VersionIdentifier;
import ai.dataeng.sqml.ingest.schema.version.VersionedDatasetSchema;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;

import java.util.HashMap;
import java.util.Map;

public class DatasetRegistration {

    private final Name name;
    private final NameCanonicalizer canonicalizer;
    private final Map<VersionIdentifier, Version> versionMapper;
//    private final VersionedDatasetSchema schema;

    public DatasetRegistration(Name name, NameCanonicalizer canonicalizer, Map<VersionIdentifier, Version> versionMapper) {
        this.name = name;
        this.canonicalizer = canonicalizer;
        this.versionMapper = versionMapper;
    }

    public DatasetRegistration(Name name, NameCanonicalizer canonicalizer) {
        this(name,canonicalizer, new HashMap<>());
    }

    public static DatasetRegistration of(String name) {
        NameCanonicalizer defaultCanonicalizer = NameCanonicalizer.SYSTEM;
        return new DatasetRegistration(Name.of(name,defaultCanonicalizer),defaultCanonicalizer);
    }

    /**
     * Each {@link SourceDataset} is uniquely identified by a name within an execution environment. Datasets are
     * imported within an SQML script by that name.
     *
     * @return Unique name of this source dataset
     */
    public Name getName() {
        return name;
    }

    public NameCanonicalizer getCanonicalizer() {
        return canonicalizer;
    }

    public Name toName(String s) {
        return Name.of(s,canonicalizer);
    }

    public boolean hasVersionId(VersionIdentifier vid) {
        return versionMapper.containsKey(vid);
    }

    public Version getVersionById(VersionIdentifier vid) {
        return versionMapper.get(vid);
    }

}
