package com.datasqrl.schema.input;

import com.datasqrl.schema.type.SqrlTypeVisitor;

public interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T, Void> {

}
