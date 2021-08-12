package ai.dataeng.sqml.dag;

/**
 * A table that is extended by defining a reference column
 */
public class ExtendedReferenceColumnTableDefinition extends AbstractExtendTableDefinition {

    private ReferenceColumnDeclaration column;

    @Override
    ColumnDeclaration addedColumn() {
        return column;
    }
}
