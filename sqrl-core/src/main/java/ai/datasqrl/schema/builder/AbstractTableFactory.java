package ai.datasqrl.schema.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import lombok.Getter;
import lombok.NonNull;

import java.util.Iterator;
import java.util.Optional;

public class AbstractTableFactory {

    @Getter
    public static class UniversalTableBuilder<T> extends NestedTableBuilder<T, UniversalTableBuilder<T>> {

        @NonNull
        final NamePath path;
        @NonNull
        final Name name;
        //The first n columns are form the primary key for this table
        // We make the assumption that primary key columns are always first!
        final int numPrimaryKeys;

        public UniversalTableBuilder(@NonNull Name name, @NonNull NamePath path, int numPrimaryKeys) {
            super(null);
            this.numPrimaryKeys = numPrimaryKeys;
            this.name = name;
            this.path = path;
        }

        public UniversalTableBuilder(@NonNull Name name, @NonNull NamePath path, UniversalTableBuilder<T> parent, boolean isSingleton) {
            super(parent);
            //Add parent primary key columns
            Iterator<Column<T>> parentCols = parent.getColumns(false,false).iterator();
            for (int i = 0; i < parent.numPrimaryKeys; i++) {
                Column<T> ppk = parentCols.next();
                addColumn(new Column<>(ppk.getName(),ppk.getVersion(),ppk.getType(),ppk.isNullable(),false));
            }
            this.numPrimaryKeys = parent.numPrimaryKeys + (isSingleton?0:1);
            this.path = path;
            this.name = name;
        }

    }

    public final class ImportBuilderFactory<T> implements UniversalTableBuilder.Factory<T, UniversalTableBuilder<T>> {

        @Override
        public UniversalTableBuilder<T> createTable(@NonNull Name name, @NonNull NamePath path, @NonNull AbstractTableFactory.UniversalTableBuilder<T> parent, boolean isSingleton) {
            return new UniversalTableBuilder(name, path.concat(name), parent, isSingleton);
        }

        @Override
        public UniversalTableBuilder<T> createTable(@NonNull Name name, @NonNull NamePath path) {
            return new UniversalTableBuilder(name, path.concat(name), 1);
        }
    }

    public static final Name parentRelationshipName = ReservedName.PARENT;

    public static Optional<Relationship> createParentRelationship(SQRLTable childTable, SQRLTable parentTable) {
        //Avoid overwriting an existing "parent" column on the child
        if (childTable.getField(parentRelationshipName).isEmpty()) {
            return Optional.of(childTable.addRelationship(parentRelationshipName, parentTable, Relationship.JoinType.PARENT,
                    Relationship.Multiplicity.ONE));
        }
        return Optional.empty();
    }


    public static Relationship createChildRelationship(Name childName, SQRLTable childTable, SQRLTable parentTable,
                                                       Relationship.Multiplicity multiplicity) {
        return parentTable.addRelationship(childName, childTable,
                Relationship.JoinType.CHILD, multiplicity);
    }


}
