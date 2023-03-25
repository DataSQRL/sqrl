/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.name;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.io.Serializable;
import java.util.function.Function;

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

  static Name changeDisplayName(Name name, String displayName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(displayName));
    return new StandardName(name.getCanonical(), displayName.trim());
  }

  static Name system(String name) {
    return of(name, NameCanonicalizer.SYSTEM);
  }

}
