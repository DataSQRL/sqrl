package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.operator.ShadowingContainer;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;

/**
 * The {@link SchemaImpl} is a logical representation of the data flow that produces the tables and fields as defined
 * in an SQRL script.
 */
@Getter
public class SchemaImpl implements SqrlSchema {
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
    public Set<Table> qualifiedTables = new HashSet<>();
}
