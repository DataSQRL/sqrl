package ai.dataeng.sqml.dag;

import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.Node;

import java.util.Map;

/**
 * A {@link ReferenceColumnDeclaration} that is defined by an explicit JOIN statement.
 */
public class JoinReferenceColumnDeclaration extends ReferenceColumnDeclaration {

    private InlineJoinBody joinDeclaration;
    private Map<Node, Scope> scopes;

    public JoinReferenceColumnDeclaration(String name, RelationDefinition referencedTable, Cardinality cardinality, InlineJoinBody joinDeclaration, Map<Node, Scope> scopes) {
        super(name, referencedTable, cardinality);
        this.joinDeclaration = joinDeclaration;
        this.scopes = scopes;
    }
}
