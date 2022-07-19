package ai.datasqrl.schema.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.Getter;
import lombok.NonNull;

import java.util.Objects;
import java.util.Optional;

@Getter
public class TableProxy<V extends VirtualTable> {

    @NonNull
    final NamePath path;
    @NonNull
    final Name id;
    @NonNull
    final FieldList fields = new FieldList();

    final V virtualTable;

    TableProxy(Name id, @NonNull NamePath path, V virtualTable) {
        this.id = id;
        this.path = path;
        this.virtualTable = virtualTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableProxy table = (TableProxy) o;
        return id.equals(table.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("Table[id=").append(id).append(",path=").append(path).append("]{\n");
        for (Field f : fields.getAccessibleFields()) s.append("\t").append(f).append("\n");
        s.append("}");
        return s.toString();
    }

    private int getNextFieldVersion(Name name) {
        return fields.nextVersion(name);
    }

    public ColumnReference addColumnReference(Name name, boolean visible) {
        ColumnReference col = new ColumnReference(name, getNextFieldVersion(name), visible);
        fields.addField(col);
        return col;
    }

    public Relationship addRelationship(Name name, TableProxy toTable,
                                        Relationship.Multiplicity multiplicity, Relationship.JoinType joinType) {
        Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, multiplicity, joinType);
        fields.addField(rel);
        return rel;
    }

    public Optional<Field> getField(Name name) {
        return getField(name,false);
    }

    public Optional<Field> getField(Name name, boolean fullColumn) {
        return fields.getAccessibleField(name);
    }

    public Optional<TableProxy<V>> walkTable(NamePath namePath) {
        if (namePath.isEmpty()) {
            return Optional.of(this);
        }
        Optional<Field> field = getField(namePath.getFirst());
        if (field.isEmpty() || !(field.get() instanceof Relationship)) {
            return Optional.empty();
        }
        Relationship<V> rel = (Relationship) field.get();
        TableProxy<V> target = rel.getToTable();
        return target.walkTable(namePath.popFirst());
    }




}
