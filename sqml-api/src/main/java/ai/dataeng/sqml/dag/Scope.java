package ai.dataeng.sqml.dag;

import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Relation;

import java.util.Map;

/**
 * The {@link Scope} is produced in the analysis phase and resolves references within SQML expressions and statements.
 */
public class Scope {

    private static final Scope NO_PARENT = null;

    /**
     * Reference to parent scope. Only available for nested expressions.
     */
    private Scope parent;
    /**
     * Maps references to relations (i.e. table names and aliases) to their definitions
     * TODO: What should be the key type?
     */
    private Map<?, RelationMapping> relationMappings;
    /**
     * Maps function calls to their definitions
     */
    private Map<FunctionCall, FunctionDefinition> functionMapping;

}
