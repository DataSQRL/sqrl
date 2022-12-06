package com.datasqrl.schema;



public interface SqrlFieldVisitor<R, C> extends FieldVisitor<R, C>{

  R visit(Relationship field, C context);
}
