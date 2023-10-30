package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import lombok.AllArgsConstructor;

/**
 * Allows for choosing the most recent table function.
 */
@AllArgsConstructor
public class SqrlNameMatcher {
  NameCanonicalizer canonicalizer;

}
