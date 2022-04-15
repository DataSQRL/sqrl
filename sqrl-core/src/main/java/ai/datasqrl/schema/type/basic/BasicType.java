package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.Type;

public interface BasicType<JavaType> extends Type {

    String getName();

    BasicType parentType();

    TypeConversion<JavaType> conversion();


}