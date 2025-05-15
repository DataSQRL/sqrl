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
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
@NoArgsConstructor(force = true)
@EqualsAndHashCode(callSuper = true)
public class FlexibleTableSchema extends FlexibleFieldSchema {

  private boolean isPartialSchema;
  @NonNull private RelationType<Field> fields;
  @NonNull private List<Constraint> constraints;

  public FlexibleTableSchema(
      Name name,
      SchemaElementDescription description,
      Object default_value,
      boolean isPartialSchema,
      RelationType<Field> fields,
      List<Constraint> constraints) {
    super(name, description, default_value);
    this.isPartialSchema = isPartialSchema;
    this.fields = fields;
    this.constraints = constraints;
  }

  @Setter
  public static class Builder extends FlexibleFieldSchema.Builder {

    protected boolean isPartialSchema = true;
    protected RelationType<Field> fields;
    protected List<Constraint> constraints = Collections.EMPTY_LIST;

    public void copyFrom(FlexibleTableSchema f) {
      super.copyFrom(f);
      isPartialSchema = f.isPartialSchema;
      fields = f.fields;
      constraints = f.constraints;
    }

    public FlexibleTableSchema build() {
      return new FlexibleTableSchema(
          name, description, default_value, isPartialSchema, fields, constraints);
    }
  }

  public static FlexibleTableSchema empty(Name name) {
    var b = new Builder();
    b.setName(name);
    b.setFields(RelationType.EMPTY);
    return b.build();
  }
}
