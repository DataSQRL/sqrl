package com.datasqrl.config;

import java.util.function.Predicate;

public class Validators {

  public static Predicate<String> minLength(int minLength) {
    return s -> s.length()>=minLength;
  }

}
