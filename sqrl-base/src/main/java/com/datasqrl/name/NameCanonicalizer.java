package com.datasqrl.name;

import java.io.Serializable;

/**
 * Produces the canonical version of a field name
 */
@FunctionalInterface
public interface NameCanonicalizer extends Serializable {

  String getCanonical(String name);

  default Name name(String name) {
    return Name.of(name, this);
  }

  NameCanonicalizer LOWERCASE_ENGLISH = new LowercaseEnglishCanonicalizer();

  NameCanonicalizer AS_IS = new IdentityCanonicalizer();

  NameCanonicalizer SYSTEM = LOWERCASE_ENGLISH;

}
