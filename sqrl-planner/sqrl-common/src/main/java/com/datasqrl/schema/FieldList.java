/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import java.util.Map.Entry;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FieldList {

  @Getter
  List<Field> fields = new ArrayList<>();

  public List<Field> toList() {
    return new ArrayList<>(fields);
  }

  public void addField(Field field) {
    fields.add(field);
  }

  public Stream<IndexedField> getIndexedFields() {
    IntStream s = IntStream.range(0, fields.size());
    return s.mapToObj(i -> new IndexedField(i, fields.get(i)));
  }

  @Value
  public static class IndexedField {

    final int index;
    final Field field;
  }


}
