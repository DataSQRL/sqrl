package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;

public class DatasetRegistration {

    private final Name name;
    private final NameCanonicalizer canonicalizer;

    public DatasetRegistration(Name name, NameCanonicalizer canonicalizer) {
        this.name = name;
        this.canonicalizer = canonicalizer;
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

}
