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
package com.datasqrl.io.schema.flexible.input;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.type.Type;
import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor(force = true)
@EqualsAndHashCode
public abstract class FlexibleFieldSchema implements SchemaField {

  @NonNull private Name name;
  @NonNull private SchemaElementDescription description;
  private Object default_value;

  @Setter
  public abstract static class Builder {

    protected Name name;
    protected SchemaElementDescription description = SchemaElementDescription.NONE;
    protected Object default_value;

    public void copyFrom(FlexibleFieldSchema f) {
      name = f.name;
      description = f.description;
      default_value = f.default_value;
    }
  }

  @Getter
  @ToString(callSuper = true)
  @NoArgsConstructor(force = true)
  @EqualsAndHashCode(callSuper = true)
  public static class Field extends FlexibleFieldSchema implements SchemaField {

    @NonNull private List<FieldType> types;

    public Field(
        Name name,
        @NonNull SchemaElementDescription description,
        Object default_value,
        List<FieldType> types) {
      super(name, description, default_value);
      this.types = types;
    }

    @Setter
    public static class Builder extends FlexibleFieldSchema.Builder {

      protected List<FieldType> types;

      public void copyFrom(Field f) {
        super.copyFrom(f);
        types = f.types;
      }

      public Field build() {
        return new Field(name, description, default_value, types);
      }
    }
  }

  @Getter
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @EqualsAndHashCode
  @ToString
  public static class FieldType implements Serializable {

    @NonNull private Name variantName;

    @NonNull private Type type;
    private int arrayDepth;

    @NonNull private List<Constraint> constraints;
  }
}
