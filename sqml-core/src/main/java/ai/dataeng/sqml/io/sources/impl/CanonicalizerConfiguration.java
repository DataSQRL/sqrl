package ai.dataeng.sqml.io.sources.impl;

import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class CanonicalizerConfiguration implements Serializable {

    private Type type = Type.DEFAULT;

    @Override
    public String toString() {
        return "CanonicalizerConfig{" + type + '}';
    }

    public static NameCanonicalizer get(CanonicalizerConfiguration config) {
        if (config==null || config.type==null) return Type.DEFAULT.canonicalizer;
        else return config.type.canonicalizer;
    }

    public enum Type {

        LOWERCASE(NameCanonicalizer.LOWERCASE_ENGLISH),
        CASE_SENSITIVE(NameCanonicalizer.AS_IS),
        DEFAULT(NameCanonicalizer.SYSTEM)
        ;

        private final NameCanonicalizer canonicalizer;

        Type(NameCanonicalizer canonicalizer) {
            this.canonicalizer = canonicalizer;
        }
    }

}
