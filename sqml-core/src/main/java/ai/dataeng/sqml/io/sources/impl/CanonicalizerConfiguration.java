package ai.dataeng.sqml.io.sources.impl;

import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import lombok.*;

import java.io.Serializable;
import java.util.Locale;

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
