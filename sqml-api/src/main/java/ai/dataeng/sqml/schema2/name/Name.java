package ai.dataeng.sqml.schema2.name;

import java.io.Serializable;

/**
 * Represents the name of a field in the ingested data
 */
public interface Name extends Serializable, Comparable<Name> {

    /**
     * Returns the canonical version of the field name.
     *
     * The canonical version of the name is used to compare field names and should
     * be used as the sole basis for the {@link #hashCode()} and {@link #equals(Object)}
     * implementations.
     *
     * @return Canonical field name
     */
    String getCanonical();

    /**
     * Returns the name to use when displaying the field (e.g. in the API).
     * This is what the user expects the field to be labeled.
     *
     * @return the display name
     */
    String getDisplay();

//    /**
//     * Returns the name of this field as used internally to make it unambiguous. This name is unique within a
//     * data pipeline.
//     *
//     * @return the unique name
//     */
//    default String getInternalName() {
//        throw new NotImplementedException("Needs to be overwritten");
//    }

    public static Name of(String name, NameCanonicalizer canonicalizer) {
        return new StandardName(canonicalizer.getCanonical(name),name);
    }

    public static final String CONCATENATE_STRING = "_";

    public static Name concatenate(Name name1, Name name2) {
        return new StandardName(name1.getCanonical()+CONCATENATE_STRING+name2.getCanonical(),
                name1.getDisplay()+CONCATENATE_STRING+name2.getDisplay());
    }

    public static Name system(String name) {
        return new SimpleName(NameCanonicalizer.SYSTEM.getCanonical(name));
    }





}
