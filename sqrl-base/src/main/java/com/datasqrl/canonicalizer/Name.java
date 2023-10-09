/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.function.Function;
import lombok.NonNull;

/**
 * Represents the name of a field in the ingested data
 */
public interface Name extends Serializable, Comparable<Name> {

  String HIDDEN_PREFIX = "_";

  String SYSTEM_HIDDEN_PREFIX = HIDDEN_PREFIX + HIDDEN_PREFIX;
  char NAME_DELIMITER = '$';

  /**
   * Returns the canonical version of the field name.
   * <p>
   * The canonical version of the name is used to compare field names and should be used as the sole
   * basis for the {@link #hashCode()} and {@link #equals(Object)} implementations.
   *
   * @return Canonical field name
   */
  String getCanonical();

  /**
   * Used to compare a name against an internal canonical name (such as column names)
   * which may have an additional version or identifier at the end (separated by {@link #NAME_DELIMITER} ).
   *
   * Should ONLY be used against internally generated name strings that are canonical.
   *
   * @param canonicalName
   * @return true if the names are identical, else false
   */
  default boolean matches(String canonicalName) {
    String canonical = getCanonical();
    return canonicalName.startsWith(canonical) &&
        (canonical.length()==canonicalName.length() ||
            (canonicalName.length()>canonical.length() && canonicalName.charAt(canonical.length())==NAME_DELIMITER));
  }

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
    return isHiddenString(getCanonical());
  }

  default boolean isSsytemHidden() {
    return getCanonical().startsWith(SYSTEM_HIDDEN_PREFIX);
  }

  default NamePath toNamePath() {
    return NamePath.of(this);
  }

  default Name append(Name name) {
    return new StandardName(this.getCanonical() + name.getCanonical(),
        this.getDisplay() + name.getCanonical());
  }

  default Name suffix(String suffix) {
    return append(system(NAME_DELIMITER + suffix));
  }

  public static String addSuffix(String str, String suffix) {
    return str + NAME_DELIMITER + suffix;
  }

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

  static Name changeDisplayName(Name name, String displayName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(displayName));
    return new StandardName(name.getCanonical(), displayName.trim());
  }

  static Name system(String name) {
    return of(name, NameCanonicalizer.SYSTEM);
  }

  static Name hidden(String name) {
    return system(hiddenString(name));
  }

  static String hiddenString(String name) {
    return HIDDEN_PREFIX + name;
  }

  static boolean isHiddenString(String name) {
    return name.startsWith(HIDDEN_PREFIX);
  }

  static boolean isSystemHidden(String name) {
    return name.startsWith(SYSTEM_HIDDEN_PREFIX);
  }

}
