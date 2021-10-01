package ai.dataeng.sqml.schema2.name;

import java.io.Serializable;
import java.util.Locale;

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
