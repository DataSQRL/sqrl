package ai.dataeng.sqml.tree.name;

import java.io.Serializable;

/**
 * Produces the canonical version of a field name
 */
@FunctionalInterface
public interface NameCanonicalizer extends Serializable {

    String getCanonical(String name);

    NameCanonicalizer LOWERCASE_ENGLISH = new LowercaseEnglishCanonicalizer();

    NameCanonicalizer AS_IS = new IdentityCanonicalizer();

    NameCanonicalizer SYSTEM = LOWERCASE_ENGLISH;

}
