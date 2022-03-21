package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.planner.operator.ShadowingContainer;

/**
 * The {@link SchemaImpl} is a logical representation of the data flow that produces the tables and fields as defined
 * in an SQRL script.
 */
public class SchemaImpl implements SqrlSchema {

    public static final String ID_DELIMITER = "$";

    /**
     * The {@link SchemaImpl#schema} represents the schema of the tables defined in an SQRL script.
     * The model is built incrementally and accounts for shadowing, i.e. adding elements to the schema with the same
     * name as previously added elements which makes those elements invisible to the API.
     *
     * The elements in the schema map to their corresponding elements in the {@link SchemaImpl}.
     * The schema produces the API schema. It is built incrementally while parsing the SQRL script and used
     * to resolve table and field references within the script.
     */
    public ShadowingContainer<DatasetOrTable> schema = new ShadowingContainer<>();
    public ShadowingContainer<DatasetOrTable> getSchema() {
        return schema;
    }
}
