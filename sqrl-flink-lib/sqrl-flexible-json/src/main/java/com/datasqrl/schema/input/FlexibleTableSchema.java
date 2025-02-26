/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.constraint.Constraint;
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
    Builder b = new Builder();
    b.setName(name);
    b.setFields(RelationType.EMPTY);
    return b.build();
  }
}
