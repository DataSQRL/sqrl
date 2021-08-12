package ai.dataeng.sqml.dag;

import ai.dataeng.sqml.tree.Relation;

import java.util.List;

/**
 * An {@link InlineRelationDefinition} is a {@link RelationDefinition} that is defined inside an expression or statement
 * (e.g. a union, sub-query, etc).
 *
 * Unlike {@link TableDefinition} an implicit definition does not have a referencable name.
 *
 * The optimizer will try to inline {@link InlineRelationDefinition} as much as possible but may have to materialize these
 * definitions as well.
 */
public class InlineRelationDefinition implements RelationDefinition {

    private Relation relationExpression;
    private Scope scope;

    @Override
    public <T extends ColumnDeclaration> List<T> getColumns(Class<T> clazz) {
        return null;
    }
}
