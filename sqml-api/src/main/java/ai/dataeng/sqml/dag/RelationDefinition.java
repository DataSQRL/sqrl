package ai.dataeng.sqml.dag;

import java.util.List;

/**
 * Defines an abstract relation in the sense of relational algebra.
 *
 * A relation is defined by a set of uniquely named columns which have a data type.
 * SQML extends the relational algebra by {@link ReferenceColumnDeclaration} which is a type of field that declares
 * a JOIN to another relation and thereby each column value for a reference field references a subset of
 * relations in the JOINed relation.
 *
 * A relation is either explicitly defined as a {@link TableDefinition} or implicitly defined inside an SQML expression
 * or statement as {@link InlineRelationDefinition}.
 */
public interface RelationDefinition {

    <T extends ColumnDeclaration> List<T> getColumns(Class<T> clazz);

    default List<ColumnDeclaration> getColumns() {
        return getColumns(ColumnDeclaration.class);
    }

}
