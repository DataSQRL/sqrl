package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import ai.dataeng.sqml.logical4.LogicalPlan.*;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Utility methods for handling and modifying {@link LogicalPlan}
 */
public class LogicalPlanUtil {

    public static <F extends Field> F getField(Table table, Name name) {
        Field f = table.fields.getByName(name);
        if (f == null) throw new NoSuchElementException("Could not find: " + name);
        return (F) f;
    }

    public static Relationship getChildRelationship(Table table, Name name) {
        Relationship r = getField(table, name);
        if (r.type != Relationship.Type.CHILD)
            throw new IllegalArgumentException("Not a child relationship: " + name);
        return r;
    }

    public static void appendOperatorForTable(Table table, RowNode addedNode) {
        table.currentNode.addConsumer(addedNode);
        table.updateNode(addedNode);
    }


    public static boolean isSource(Node node) {
        return node instanceof DocumentSource;
    }

    public static Table getTable(Column[] inputs) {
        Preconditions.checkArgument(inputs!=null && inputs.length>0);
        final Table table = inputs[0].getTable();
        assert Arrays.stream(inputs).map(c -> c.getTable()).filter(t -> !t.equals(table)).count() == 0;
        return table;
    }

    public static Column copyColumnToTable(Column column, Table table, Name name, boolean isPrimary) {
        Column copy = new Column(name,table,getNextVersion(table, name),
                column.type, column.arrayDepth,
                List.copyOf(column.constraints),isPrimary,column.isSystem);
        table.addField(copy);
        return copy;
    }
    
    public static int getNextVersion(Table table, Name fieldName) {
        Field f = table.getField(fieldName);
        int oldVersion = -1;
        if (f==null) return 0;
        else if (f instanceof Column) {
            oldVersion = ((Column)f).version;
        } else if (f instanceof Relationship) {
            //This is an unusual situation where a column overwrites a relationship
            //We might not allow this. If we do, we have to check if there are older versions
            for (Field anyField : table.getFields()) {
                if (fieldName.equals(anyField.getName()) && anyField instanceof Column) {
                    oldVersion = Math.max(oldVersion,((Column)anyField).version);
                }
            }
        } else throw new UnsupportedOperationException("Unexpected field type: " + f);
        return oldVersion+1; //next version
    }


}
