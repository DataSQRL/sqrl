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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

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

/**
 * {@link QualifiedName} is used to identify a field/column within a nested structure. It specifies the full path.
 */
public class QualifiedName {

  private final List<String> normalizedParts;
  private final List<String> originalParts;

  private QualifiedName(List<String> originalParts, List<String> parts) {
    this.originalParts = originalParts;
    this.normalizedParts = parts;
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
    checkArgument(!isEmpty(originalParts), "originalParts is empty");
    List<String> parts = ImmutableList
        .copyOf(transform(originalParts, QualifiedName::normalizeName));

    return new QualifiedName(ImmutableList.copyOf(originalParts), parts);
  }

  public static QualifiedName of(Identifier node) {
    String[] parts = node.getValue().split("\\.");
    return of(Arrays.asList(parts));
  }

  public List<String> getParts() {
    return normalizedParts;
  }

  public List<String> getOriginalParts() {
    return originalParts;
  }

  @Override
  public String toString() {
    return Joiner.on('.').join(normalizedParts);
  }

  /**
   * For an identifier of the form "a.b.c.d", returns "a.b.c" For an identifier of the form "a",
   * returns absent
   */
  public Optional<QualifiedName> getPrefix() {
    if (normalizedParts.size() == 1) {
      return Optional.empty();
    }

    List<String> subList = normalizedParts.subList(0, normalizedParts.size() - 1);
    return Optional.of(new QualifiedName(subList, subList));
  }

  public boolean hasSuffix(QualifiedName suffix) {
    if (normalizedParts.size() < suffix.getParts().size()) {
      return false;
    }

    int start = normalizedParts.size() - suffix.getParts().size();

    return normalizedParts.subList(start, normalizedParts.size()).equals(suffix.getParts());
  }

  public String getSuffix() {
    return Iterables.getLast(normalizedParts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return normalizedParts.equals(((QualifiedName) o).normalizedParts);
  }

  @Override
  public int hashCode() {
    return normalizedParts.hashCode();
  }

  public static String normalizeName(String original) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(original),"Invalid name: %s", original);
    //TODO: What other naming requirements do we have?
    return original.toLowerCase(ENGLISH);
  }

  /**
   * Gets the parent scope or the root
   */
  public QualifiedName getParent() {
    return this.getPrefix().orElse(this);
  }

  public QualifiedName withSuffix(String part) {
    return QualifiedName.of(Iterables.concat(this.getParts(), List.of(part)));
  }

  public NamePart getFirst() {
    String part = this.getParts().get(0);
    if (part.equalsIgnoreCase("@")) {
      return new SelfPart(part);
    } else if (part.equalsIgnoreCase("parent")) {
      return new ParentPart(part);
    } else if (part.equalsIgnoreCase("sibling")) {
      return new SibilingPart(part);
    } else {
      return new DataPart(part);
    }
  }

  public QualifiedName getRest() {
    return QualifiedName.of(this.getParts().subList(1, this.getParts().size()));
  }

  public QualifiedName append(String name) {
    List<String> newName = new ArrayList<>(this.getParts());
    newName.add(name);

    return QualifiedName.of(newName);
  }

  public static abstract class NamePart {
    public String name;
    public NamePart(String name) {
      this.name = name;
    }
    public <R, C> R accept(QualifiedNameVisitor<R, C> visitor, C context) {
      return visitor.visitNamePart(this, context);
    }
  }

  public static class ParentPart extends NamePart {
    public ParentPart(String name) {
      super(name);
    }

    public <R, C> R accept(QualifiedNameVisitor<R, C> visitor, C context) {
      return visitor.visitParentPart(this, context);
    }
  }
  public static class SelfPart extends NamePart {
    public SelfPart(String name) {
      super(name);
    }

    public <R, C> R accept(QualifiedNameVisitor<R, C> visitor, C context) {
      return visitor.visitSelfPart(this, context);
    }
  }
  public static class SibilingPart extends NamePart {
    public SibilingPart(String name) {
      super(name);
    }

    public <R, C> R accept(QualifiedNameVisitor<R, C> visitor, C context) {
      return visitor.visitSibilingPart(this, context);
    }
  }
  public static class DataPart extends NamePart {
    public DataPart(String name) {
      super(name);
    }

    public <R, C> R accept(QualifiedNameVisitor<R, C> visitor, C context) {
      return visitor.visitDataPart(this, context);
    }
  }
  public static abstract class QualifiedNameVisitor<R, C> {
    public R visitNamePart(NamePart dataPart, C context) {
      return null;
    }
    public abstract R visitDataPart(DataPart dataPart, C context);
    public abstract R visitParentPart(ParentPart dataPart, C context);
    public abstract R visitSelfPart(SelfPart dataPart, C context);
    public abstract R visitSibilingPart(SibilingPart dataPart, C context);
  }
}
