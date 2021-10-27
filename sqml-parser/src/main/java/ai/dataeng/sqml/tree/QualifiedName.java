/*
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
package ai.dataeng.sqml.tree;

import static com.google.common.collect.Iterables.transform;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link QualifiedName} is used to identify a field/column within a nested structure. It specifies the full path.
 */
public class QualifiedName {

  private final List<Name> nameParts;

  private QualifiedName(List<Name> nameParts) {
    this.nameParts = nameParts;
  }

  public static QualifiedName of(String first, String... rest) {
    requireNonNull(first, "first is null");
    return of(ImmutableList.copyOf(Lists.asList(first, rest)));
  }

  public static QualifiedName of(String name) {
    requireNonNull(name, "name is null");
    return of(ImmutableList.of(name));
  }

  public static QualifiedName of(Iterable<String> originalParts) {
    requireNonNull(originalParts, "originalParts is null");
    List<Name> parts = ImmutableList
        .copyOf(transform(originalParts, QualifiedName::normalizeName));

    return new QualifiedName(parts);
  }

  public static QualifiedName of() {
    return of(List.of());
  }

  public static QualifiedName of(Identifier node) {
    String[] parts = node.getValue().split("\\.");
    return of(Arrays.asList(parts));
  }

  public List<String> getParts() {
    return nameParts.stream()
        .map(e->e.getCanonical())
        .collect(Collectors.toList());
  }

  public List<String> getOriginalParts() {
    return nameParts.stream()
        .map(e->e.getDisplay())
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return Joiner.on('.').join(nameParts);
  }

  /**
   * For an identifier of the form "a.b.c.d", returns "a.b.c" For an identifier of the form "a",
   * returns absent
   */
  public Optional<QualifiedName> getPrefix() {
    if (nameParts.size() <= 1) {
      return Optional.empty();
    }

    List<Name> subList = nameParts.subList(0, nameParts.size() - 1);
    return Optional.of(new QualifiedName(subList));
  }

  public Name getSuffix() {
    return Iterables.getLast(nameParts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return nameParts.equals(((QualifiedName) o).nameParts);
  }

  @Override
  public int hashCode() {
    return nameParts.hashCode();
  }

  public static Name normalizeName(String original) {
    return Name.system(original);
  }

  /**
   * Gets the parent scope or the root
   */
  public QualifiedName getParent() {
    return this.getPrefix().orElse(this);
  }
}
