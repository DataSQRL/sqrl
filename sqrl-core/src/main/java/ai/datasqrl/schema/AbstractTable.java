package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@AllArgsConstructor
public class AbstractTable implements ShadowingContainer.Element {

    protected final int uniqueId;
    @NonNull
    protected final NamePath path;
    @NonNull protected final ShadowingContainer<Field> fields;


    public Name getId() {
        return getName().suffix(Integer.toString(uniqueId));
    }

    @Override
    public Name getName() {
        return path.getLast();
    }

    public boolean isVisible() {
        return true;
    }

    public int getVersion() {
        return uniqueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        return uniqueId == table.uniqueId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId);
    }

    @Override
    public String toString() {
        return "Table{" +
                "name=" + getName() +
                '}';
    }

    public Optional<Field> getField(Name name) {
        return fields.getByName(name);
    }

    public Stream<Column> getAllColumns() {
        return fields.stream().filter(Column.class::isInstance).map(Column.class::cast);
    }

    public int getNextColumnVersion(Name name) {
        return getAllColumns().filter(c -> c.getName().equals(name)).map(Column::getVersion).max(Integer::compareTo)
                .map(i -> i+1).orElse(0);
    }

    public List<Column> getPrimaryKeys() {
        return getAllColumns().filter(Column::isPrimaryKey).collect(Collectors.toList());
    }

}
