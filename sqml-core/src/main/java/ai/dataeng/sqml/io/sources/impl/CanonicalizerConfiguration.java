package ai.dataeng.sqml.io.sources.impl;

import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
public class CanonicalizerConfiguration implements Serializable {

    @Builder.Default
    @NonNull Type type = Type.DEFAULT;

    @Override
    public String toString() {
        return "CanonicalizerConfig{" + type + '}';
    }

    public NameCanonicalizer initialize() {
        return type.canonicalizer;
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
