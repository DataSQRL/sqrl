/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.canonicalizer;

import java.io.Serializable;

/** Represents the name of a field in the ingested data */
public interface Name extends Serializable, Comparable<Name> {

  char NAME_DELIMITER = '_';
  String HIDDEN_PREFIX = "_";
  String SYSTEM_HIDDEN_PREFIX = HIDDEN_PREFIX + HIDDEN_PREFIX;

  /**
   * Returns the canonical version of the field name.
   *
   * <p>
   *
   * @return Canonical field name
   */
  String getCanonical();

  /**
   * Used to compare a name against an internal canonical name (such as column names) which may have
   * an additional version or identifier at the end (separated by {@link #NAME_DELIMITER} ).
   *
   * <p>Should ONLY be used against internally generated name strings that are canonical.
   *
   * @param canonicalName
   * @return true if the names are identical, else false
   */
  default boolean matches(String canonicalName) {
    var canonical = getCanonical();
    return canonicalName.startsWith(canonical)
        && (canonical.length() == canonicalName.length()
            || (canonicalName.length() > canonical.length()
                && canonicalName.charAt(canonical.length()) == NAME_DELIMITER));
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

  default Name append(Name name) {
    return new StandardName(
        this.getCanonical() + name.getCanonical(), this.getDisplay() + name.getCanonical());
  }

  static boolean validName(String name) {
    return !(name == null
        || name.trim().isEmpty()); // && name.indexOf('.') < 0 && name.indexOf('/') < 0;
  }

  static boolean isValidNameStrict(String name) {
    return !(name == null || name.trim().isEmpty())
        && name.indexOf('.') < 0
        && name.indexOf('/') < 0;
  }

  static Name of(String name, NameCanonicalizer canonicalizer) {
    name = name.trim();
    return new StandardName(canonicalizer.getCanonical(name), name);
  }

  static Name changeDisplayName(Name name, String displayName) {
    return new StandardName(name.getCanonical(), displayName.trim());
  }

  static Name system(String name) {
    return of(name, NameCanonicalizer.SYSTEM);
  }

  static Name lower(String name) {
    return of(name, NameCanonicalizer.LOWERCASE_ENGLISH);
  }

  static boolean isHiddenString(String name) {
    return name.startsWith(HIDDEN_PREFIX);
  }

  static boolean isSystemHidden(String name) {
    return name.startsWith(SYSTEM_HIDDEN_PREFIX);
  }
}
