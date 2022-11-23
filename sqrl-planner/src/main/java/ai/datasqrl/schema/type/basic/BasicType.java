package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.Type;

public interface BasicType<JavaType> extends Type, Comparable<BasicType> {

  String getName();

  TypeConversion<JavaType> conversion();


}