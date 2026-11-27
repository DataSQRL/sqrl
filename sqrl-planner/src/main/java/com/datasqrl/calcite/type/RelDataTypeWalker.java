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
package com.datasqrl.calcite.type;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Walks a type and its nested types bottom-up, and maps each of them to a type of another type
 * system. A type that is mapped to nothing is left out of the type that contains it.
 *
 * <p>The walker does not handle nullability, because the type systems express it differently.
 *
 * @param <T> the type of the other type system
 */
public abstract class RelDataTypeWalker<T> {

  /**
   * @param type the type to map
   * @param path the path of the type: the path of a field is the path of its row followed by the
   *     field name, and the elements of an array have the path of the array
   * @return the mapped type, empty if the type has no mapping
   */
  public final Optional<T> walk(RelDataType type, NamePath path) {
    var kind = TypeKind.of(type);

    return switch (kind) {
      case ROW -> visitRow(type, walkFields(type, path), path);
      case ARRAY ->
          walk(type.getComponentType(), path).flatMap(element -> visitArray(type, element, path));
      case MAP -> visitMap(type, path);
      default -> visitScalar(type, kind, path);
    };
  }

  private List<Field<T>> walkFields(RelDataType rowType, NamePath path) {
    var fields = new ArrayList<Field<T>>();
    for (var field : rowType.getFieldList()) {
      var fieldPath = path.concat(Name.system(field.getName()));
      if (includeField(field, fieldPath)) {
        walk(field.getType(), fieldPath).ifPresent(value -> fields.add(new Field<>(field, value)));
      }
    }
    return fields;
  }

  /** Whether the field is part of the mapped row. */
  protected boolean includeField(RelDataTypeField field, NamePath fieldPath) {
    return true;
  }

  /**
   * Maps a type that has no nested types.
   *
   * @param kind the kind of the type, which is not ROW, ARRAY or MAP
   */
  protected abstract Optional<T> visitScalar(RelDataType type, TypeKind kind, NamePath path);

  /**
   * @param fields the fields of the row that are included and have a mapping, with their mapping
   */
  protected abstract Optional<T> visitRow(RelDataType type, List<Field<T>> fields, NamePath path);

  /**
   * @param element the mapped element type
   */
  protected abstract Optional<T> visitArray(RelDataType type, T element, NamePath path);

  /** Maps a map type. The implementation decides whether it walks the key and value types. */
  protected abstract Optional<T> visitMap(RelDataType type, NamePath path);

  public record Field<T>(RelDataTypeField field, T value) {}
}
