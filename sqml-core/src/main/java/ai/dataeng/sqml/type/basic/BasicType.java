package ai.dataeng.sqml.type.basic;

import ai.dataeng.sqml.type.Type;

public interface BasicType<JavaType> extends Type {

    String getName();

    BasicType parentType();

    TypeConversion<JavaType> conversion();


}