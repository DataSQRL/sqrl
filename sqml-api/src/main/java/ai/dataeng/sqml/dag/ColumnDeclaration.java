package ai.dataeng.sqml.dag;

import java.util.Locale;
import java.util.Objects;

/**
 * A {@link ColumnDeclaration} declares the meta-data of a field as defined on a table as part of a {@link RelationDefinition}.
 *
 * A field has a unique (local to the table) name that is not case sensitive. A field can either be a {@link DataColumnDeclaration}
 * when it represents a scalar or array data value or a {@link ReferenceColumnDeclaration} when it declares a reference
 * to another table.
 */
public abstract class ColumnDeclaration {

    private final String name;

    public ColumnDeclaration(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Column[" + name + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnDeclaration that = (ColumnDeclaration) o;
        return name.equalsIgnoreCase(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name.toLowerCase(Locale.ENGLISH));
    }
}
