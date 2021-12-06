package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.tree.name.Name;

import java.util.NoSuchElementException;

public class LogicalPlanUtil {

    public static <F extends LogicalPlan.Field> F getField(LogicalPlan.Table table, Name name) {
        LogicalPlan.Field f = table.fields.getByName(name);
        if (f == null) throw new NoSuchElementException("Could not find: " + name);
        return (F) f;
    }

    public static LogicalPlan.Relationship getChildRelationship(LogicalPlan.Table table, Name name) {
        LogicalPlan.Relationship r = getField(table, name);
        if (r.type != LogicalPlan.Relationship.Type.CHILD)
            throw new IllegalArgumentException("Not a child relationship: " + name);
        return r;
    }


}
