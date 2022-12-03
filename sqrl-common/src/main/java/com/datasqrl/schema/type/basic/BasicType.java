package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.Type;

public interface BasicType<JavaType> extends Type, Comparable<BasicType> {

  String getName();

  TypeConversion<JavaType> conversion();


}