package ai.dataeng.sqml.dag;

/**
 * A mapping to a {@link RelationDefinition}.
 *
 * We introduce mappings as objects so that we can update the mapping easily during query rewriting when relations are flattened
 * or consolidated.
 */
public class RelationMapping {

    private RelationDefinition mapsTo;

}
