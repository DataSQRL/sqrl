/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

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
