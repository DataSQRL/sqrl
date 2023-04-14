/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.constraint;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.type.Type;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Unique implements Constraint {

  public static final Name NAME = Name.system("unique");

  public static final Unique UNCONSTRAINED = new Unique();

  private Unique() {
  } //For Kryo

  @Override
  public boolean satisfies(Object value) {
    return true;
  }

  @Override
  public boolean appliesTo(Type type) {
    return false;
  }

  @Override
  public Name getName() {
    return NAME;
  }

  @Override
  public Map<String, Object> export() {
    return Map.of();
  }

  @Override
  public String toString() {
    return NAME.getDisplay();
  }

  public static class Factory implements Constraint.Factory {

    @Override
    public Name getName() {
      return NAME;
    }

    @Override
    public Optional<Constraint> create(Map<String, Object> parameters, ErrorCollector errors) {
      return Optional.of(new Unique());
    }
  }
}
