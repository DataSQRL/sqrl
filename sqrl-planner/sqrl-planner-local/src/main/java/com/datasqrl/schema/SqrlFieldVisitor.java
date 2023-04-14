/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;



public interface SqrlFieldVisitor<R, C> extends FieldVisitor<R, C>{

  R visit(Relationship field, C context);
}
