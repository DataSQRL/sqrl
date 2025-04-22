/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.input;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.schema.flexible.type.Type;

import lombok.NonNull;

public class RelationType<F extends SchemaField> implements Type, Iterable<F> {

  public static final RelationType EMPTY = new RelationType();

  protected final List<F> fields;

  public RelationType() {
    this(new ArrayList<>());
  }

  public RelationType(@NonNull List<F> fields) {
    //Preconditions.checkArgument(!fields.isEmpty()); TODO: should this be checked?
    this.fields = fields;
  }

  //Lazily initialized when requested because this only works for fields with names
  protected transient Map<Name, F> fieldsByName = null;

  /**
   * Returns a field with the given name or null if such does not exist. If two fields have the same
   * name, it returns the one added last (i.e. has the highest index in the array)
   *
   * @param name
   * @return
   */
  public Optional<F> getFieldByName(Name name) {
    if (fieldsByName == null) {
      fieldsByName = fields.stream().collect(
          Collectors.toUnmodifiableMap(t -> t.getName(), Function.identity(),
              (v1, v2) -> v2));
    }
    return Optional.ofNullable(fieldsByName.get(name));
  }

  public void add(F field) {
    fields.add(field);
    //Need to reset fieldsByName so this new field can be found
    fieldsByName = null;
  }

  @Override
  public String toString() {
    return "{" + fields.stream().map(f -> f.toString()).collect(Collectors.joining("; ")) + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationType<?> that = (RelationType<?>) o;
    return fieldsOrderedByName().equals(that.fieldsOrderedByName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldsOrderedByName());
  }

  private List<F> fieldsOrderedByName() {
    List<F> ordered = new ArrayList<>(fields);
    ordered.sort((a, b) -> a.getName().compareTo(b.getName()));
    return ordered;
  }

  public static <F extends FlexibleFieldSchema> Builder<F> build() {
    return new Builder<>();
  }

  public List<F> getFields() {
    return fields;
  }

  @Override
  public Iterator<F> iterator() {
    return fields.iterator();
  }

  public static class Builder<F extends FlexibleFieldSchema> extends AbstractBuilder<F, Builder<F>> {

    public Builder() {
      super(true);
    }

    public RelationType<F> build() {
      return new RelationType<>(fields);
    }
  }

  protected static class AbstractBuilder<F extends FlexibleFieldSchema, B extends AbstractBuilder<F, B>> {

    protected final List<F> fields = new ArrayList<>();
    protected final Set<Name> fieldNames;

    public AbstractBuilder(boolean checkFieldNameUniqueness) {
      if (checkFieldNameUniqueness) {
        fieldNames = new HashSet<>();
      } else {
        fieldNames = null;
      }
    }

    public boolean hasFieldWithName(@NonNull Name name) {
//      Preconditions.checkArgument(fieldNames != null);
      return fieldNames.contains(name);
    }

    public B add(@NonNull F field) {
//      Preconditions.checkArgument(fieldNames == null || !fieldNames.contains(field.getName()));
      fields.add(field);
      if (fieldNames != null) {
        fieldNames.add(field.getName());
      }
      return (B) this;
    }

    public B addAll(RelationType<F> copyFrom) {
      for (F f : copyFrom) {
        add(f);
      }
      return (B) this;
    }

  }
}
