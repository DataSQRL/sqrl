package ai.datasqrl.parse.tree.name;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.function.Function;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

/**
 * Represents the name of a field in the ingested data
 */
public interface Name extends Serializable, Comparable<Name> {

  String HIDDEN_PREFIX = "_";
  Name SELF_IDENTIFIER = Name.system("_");


  /**
   * Returns the canonical version of the field name.
   * <p>
   * The canonical version of the name is used to compare field names and should be used as the sole
   * basis for the {@link #hashCode()} and {@link #equals(Object)} implementations.
   *
   * @return Canonical field name
   */
  String getCanonical();

  default int length() {
    return getCanonical().length();
  }

  /**
   * Returns the name to use when displaying the field (e.g. in the API). This is what the user
   * expects the field to be labeled.
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

  static boolean validName(String name) {
    return !Strings.isNullOrEmpty(name);// && name.indexOf('.') < 0 && name.indexOf('/') < 0;
  }

  static <T> T getIfValidName(@NonNull String name, @NonNull NameCanonicalizer canonicalizer,
      @NonNull Function<Name, T> getter) {
    if (!validName(name)) {
      return null;
    }
    return getter.apply(canonicalizer.name(name));
  }

  static <T> T getIfValidSystemName(String name, Function<Name, T> getter) {
    return getIfValidName(name, NameCanonicalizer.SYSTEM, getter);
  }

  static Name of(String name, NameCanonicalizer canonicalizer) {
    Preconditions.checkArgument(validName(name), "Invalid name: %s", name);
    name = name.trim();
    return new StandardName(canonicalizer.getCanonical(name), name);
  }

  static Name ofCanonical(String canonicalName) {
    return new SimpleName(canonicalName);
  }

  static Name changeDisplayName(Name name, String displayName) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(displayName));
    return new StandardName(name.getCanonical(), displayName.trim());
  }

  String COMBINATION_SEPERATOR = "_";

  static Name combine(Name name1, Name name2) {
    return new StandardName(name1.getCanonical() + COMBINATION_SEPERATOR + name2.getCanonical(),
        name1.getDisplay() + COMBINATION_SEPERATOR + name2.getDisplay());
  }

  static Name system(String name) {
    return of(name, NameCanonicalizer.SYSTEM);
  }

  static Name hidden(String name) {
    return system(HIDDEN_PREFIX + name);
  }

  default NamePath toNamePath() {
    return NamePath.of(this);
  }
}
