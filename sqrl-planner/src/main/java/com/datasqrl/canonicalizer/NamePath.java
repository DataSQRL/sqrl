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
  protected AbstractPath.Constructor<Name, NamePath> constructor() {
    return CONSTRUCTOR;
  }

  public String getDisplay() {
    return Arrays.stream(elements).map(e -> e.getDisplay()).collect(Collectors.joining("."));
  }

  public Name[] getNames() {
    return elements;
  }

  public List<String> toStringList() {
    return Arrays.stream(this.elements).map(n -> n.getDisplay()).collect(Collectors.toList());
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
