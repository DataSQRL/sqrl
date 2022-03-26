package ai.dataeng.sqml.tree.name;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Represents the name of a field in the ingested data
 */
public interface Name extends Serializable, Comparable<Name> {

    public static final String HIDDEN_PREFIX = "_";
    public static final Name SELF_IDENTIFIER = Name.system("_");
    public static final Name PARENT_RELATIONSHIP = Name.system("parent");
    public static final Name SIBLING_RELATIONSHIP = Name.system("sibling");


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

    default int length() {
        return getCanonical().length();
    }

    /**
     * Returns the name to use when displaying the field (e.g. in the API).
     * This is what the user expects the field to be labeled.
     *
     * @return the display name
     */
    String getDisplay();

    default boolean isHidden() {
        return getCanonical().startsWith(HIDDEN_PREFIX);
    }

//    /**
//     * Returns the name of this field as used internally to make it unambiguous. This name is unique within a
//     * data pipeline.
//     *
//     * @return the unique name
//     */
//    default String getInternalName() {
//        throw new NotImplementedException("Needs to be overwritten");
//    }

    public static boolean validName(String name) {
        return !Strings.isNullOrEmpty(name) && name.indexOf(46)<0 && name.indexOf(47)<0;
    }

    public static Name of(String name, NameCanonicalizer canonicalizer) {
        Preconditions.checkArgument(validName(name),"Invalid name: %s",name);
        name = name.trim();
        return new StandardName(canonicalizer.getCanonical(name),name);
    }

    public static Name changeDisplayName(Name name, String displayName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(displayName));
        return new StandardName(name.getCanonical(),displayName.trim());
    }

    public static final String COMBINATION_SEPERATOR = "_";

    public static Name combine(Name name1, Name name2) {
        return new StandardName(name1.getCanonical()+ COMBINATION_SEPERATOR +name2.getCanonical(),
                name1.getDisplay()+ COMBINATION_SEPERATOR +name2.getDisplay());
    }

    public static Name system(String name) {
        return new SimpleName(NameCanonicalizer.SYSTEM.getCanonical(name));
    }

    public static Name hidden(String name) {
        return system(HIDDEN_PREFIX+name);
    }

    public default NamePath toNamePath() {
        return NamePath.of(this);
    }


}
