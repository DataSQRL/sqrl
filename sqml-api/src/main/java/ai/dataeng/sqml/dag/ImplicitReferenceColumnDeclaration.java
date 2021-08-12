package ai.dataeng.sqml.dag;

import com.google.common.base.Preconditions;

/**
 * When a table is defined within another table as a nested table, this introduces
 * a parent-child relationship which is captured by automatically adding certain
 * {@link ReferenceColumnDeclaration} on the parent and child tables:
 *
 * <ul>
 *     <li>PARENT: from child to parent table. This reference column is always named "parent".</li>
 *     <li>CHILDREN: from parent to child table. This reference column is named after the child table.</li>
 *     <li>SIBLINGS: self-join on the child table for all rows that share the same parent. This reference columns is always named "siblings".</li>
 * </ul>
 */
public class ImplicitReferenceColumnDeclaration extends ReferenceColumnDeclaration {

    private final Type type;

    public ImplicitReferenceColumnDeclaration(String name, RelationDefinition referencedTable, Cardinality cardinality, Type type) {
        super(name, referencedTable, cardinality);
        Preconditions.checkArgument(type!=Type.PARENT || cardinality==Cardinality.SINGLE, "Parent relationships must have single cardinality: %s", cardinality);
        this.type = type;
    }


    public enum Type {

        PARENT, CHILDREN, SIBLINGS;

    }

}
