package ai.datasqrl.schema.input;

import ai.datasqrl.schema.builder.SimpleTableBuilder;

public abstract class SimpleFlexibleTableConverterVisitor<T> extends TableBuilderFlexibleTableConverterVisitor<T, SimpleTableBuilder<T>> {

    public SimpleFlexibleTableConverterVisitor() {
        super(new SimpleTableBuilder.Factory<>());
    }
}
