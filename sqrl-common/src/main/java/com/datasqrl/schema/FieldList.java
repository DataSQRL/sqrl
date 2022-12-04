/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.name.Name;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FieldList {

  List<Field> fields = new ArrayList<>();

  public int nextVersion(Name name) {
    return fields.stream().filter(f -> f.getName().equals(name)).map(Field::getVersion)
        .max(Integer::compareTo).map(i -> i + 1).orElse(0);
  }

  public List<Field> toList() {
    return new ArrayList<>(fields);
  }

  public Field atIndex(int index) {
    return fields.get(index);
  }

  public int numFields() {
    return fields.size();
  }

  public void addField(Field field) {
//        Preconditions.checkArgument(field.getVersion()>=nextVersion(field.getName()));
    fields.add(field);
  }

  public Stream<Field> getFields(boolean onlyVisible) {
    Stream<Field> s = fields.stream();
    if (onlyVisible) {
      s = s.filter(Field::isVisible);
    }
    return s;
  }

  public Stream<IndexedField> getIndexedFields(boolean onlyVisible) {
    IntStream s = IntStream.range(0, fields.size());
    if (onlyVisible) {
      s = s.filter(i -> fields.get(i).isVisible());
    }
    return s.mapToObj(i -> new IndexedField(i, fields.get(i)));
  }

  public List<Field> getAccessibleFields() {
    Map<Name, Field> fieldsByName = getFields(true)
        .collect(Collectors.toMap(Field::getName, Function.identity(),
            BinaryOperator.maxBy(Comparator.comparing(Field::getVersion))));
    return getFields(true).filter(f -> fieldsByName.get(f.getName()).equals(f))
        .collect(Collectors.toList());
  }

  public Optional<Field> getAccessibleField(@NonNull Name name) {
    return fields.stream().filter(f -> f.getName().equals(name))
        .filter(Field::isVisible)
        .max((a, b) -> Integer.compare(a.getVersion(), b.getVersion()));
  }

  @Value
  public static class IndexedField {

    final int index;
    final Field field;
  }


}
