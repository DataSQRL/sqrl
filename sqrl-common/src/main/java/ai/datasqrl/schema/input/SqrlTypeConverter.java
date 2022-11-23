package ai.datasqrl.schema.input;

import ai.datasqrl.schema.type.SqrlTypeVisitor;

public interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T, Void> {

}
