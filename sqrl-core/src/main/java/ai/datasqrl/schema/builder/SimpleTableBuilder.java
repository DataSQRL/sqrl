package ai.datasqrl.schema.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;


public class SimpleTableBuilder<T> extends NestedTableBuilder<T,SimpleTableBuilder<T>> {

    final Name name;
    final NamePath namePath;

    protected SimpleTableBuilder(Name name, NamePath namePath) {
        super(null);
        this.name = name;
        this.namePath = namePath;
    }

    public static class Factory<T> implements TableBuilder.Factory<T,SimpleTableBuilder<T>> {

        @Override
        public SimpleTableBuilder<T> createTable(Name name, NamePath path, SimpleTableBuilder<T> parent, boolean isSingleton) {
            return createTable(name,path);
        }

        @Override
        public SimpleTableBuilder<T> createTable(Name name, NamePath path) {
            return new SimpleTableBuilder<>(name,path);
        }
    }

}
