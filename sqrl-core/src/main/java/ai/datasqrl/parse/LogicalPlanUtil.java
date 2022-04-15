package ai.datasqrl.parse;

import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class LogicalPlanUtil {

    public static <F extends Field> F getField(Table table, Name name) {
        Field f = table.fields.getByName(name).get();
        if (f == null) throw new NoSuchElementException("Could not find: " + name);
        return (F) f;
    }

    public static Relationship getChildRelationship(Table table, Name name) {
        Relationship r = getField(table, name);
        if (r.type != Relationship.Type.CHILD)
            throw new IllegalArgumentException("Not a child relationship: " + name);
        return r;
    }

    public static Table getTable(Column[] inputs) {
        Preconditions.checkArgument(inputs!=null && inputs.length>0);
        final Table table = inputs[0].getTable();
        assert Arrays.stream(inputs).map(c -> c.getTable()).filter(t -> !t.equals(table)).count() == 0;
        return table;
    }
}
