package ai.dataeng.sqml.io.sources.impl;

import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import java.io.Serializable;
import java.util.Locale;
import lombok.Getter;

@Getter
public enum CanonicalizerConfiguration implements Serializable {

    lowercase(NameCanonicalizer.LOWERCASE_ENGLISH),
    case_sensitive(NameCanonicalizer.AS_IS),
    system(NameCanonicalizer.SYSTEM)
    ;

    private final NameCanonicalizer canonicalizer;

    CanonicalizerConfiguration(NameCanonicalizer canonicalizer) {
        this.canonicalizer = canonicalizer;
    }



    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

}
