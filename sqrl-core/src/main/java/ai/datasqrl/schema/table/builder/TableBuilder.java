package ai.datasqrl.schema.table.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.table.Relationship;
import lombok.NonNull;

public interface TableBuilder<T, X extends TableBuilder<T,X>> {

    public void addColumn(@NonNull Name name, @NonNull T type, boolean nullable);

    public void addChild(@NonNull Name name, @NonNull X child, @NonNull T type, Relationship.Multiplicity multiplicity);

    interface Factory<T, X extends TableBuilder<T,X>> {

        X createTable(@NonNull Name name, @NonNull NamePath path, @NonNull X parent, boolean isSingleton);

        X createTable(@NonNull Name name, @NonNull NamePath path);

    }

}
