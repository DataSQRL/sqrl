package ai.dataeng.sqml.execution.flink.ingest;

import ai.dataeng.sqml.execution.flink.ingest.source.SourceDataset;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;

import java.io.Serializable;

public class DatasetRegistration implements Serializable {

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
