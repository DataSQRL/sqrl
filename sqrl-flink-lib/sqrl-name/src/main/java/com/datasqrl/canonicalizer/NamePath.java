/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.canonicalizer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

public final class NamePath extends AbstractPath<Name, NamePath> {

  public static final NamePath ROOT = new NamePath();
  private static final Constructor CONSTRUCTOR = new Constructor();

  private NamePath(@NonNull Name... names) {
    super(names);
  }

  public static NamePath system(List<String> names) {
    return of(names.toArray(String[]::new));
  }

  @Override
  protected Constructor constructor() {
    return CONSTRUCTOR;
  }

  public String getDisplay() {
    return Arrays.stream(elements)
        .map(e -> e.getDisplay())
        .collect(Collectors.joining("."));
  }

  public Name[] getNames() {
    return elements;
  }

  public List<String> toStringList() {
    return Arrays.stream(this.elements)
        .map(n->n.getDisplay())
        .collect(Collectors.toList());
  }

  private static final class Constructor extends AbstractPath.Constructor<Name, NamePath> {

    @Override
    protected NamePath create(@NonNull Name... elements) {
      return new NamePath(elements);
    }

    @Override
    protected Name[] createArray(int length) {
      return new Name[length];
    }

    @Override
    protected NamePath root() {
      return ROOT;
    }

  }

  public static NamePath of(@NonNull Name... names) {
    return new NamePath(names);
  }

  public static NamePath of(@NonNull List<Name> names) {
    return CONSTRUCTOR.of(names);
  }

  public static NamePath of(@NonNull String... names) {
    return CONSTRUCTOR.of(Name::system, names);
  }

  public static NamePath parse(String path) {
    return CONSTRUCTOR.parse(path, s -> Name.of(s, NameCanonicalizer.SYSTEM));
  }

}
