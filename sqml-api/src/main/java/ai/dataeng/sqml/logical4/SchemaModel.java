package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.tree.name.Name;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

/**
 * The {@link SchemaModel} represents the schema of the tables defined in an SQRL script.
 * The model is built incrementally and accounts for shadowing, i.e. adding elements to the schema with the same
 * name as previously added elements which makes those elements invisible to the API.
 *
 * The elements in the {@link SchemaModel} map to their corresponding elements in the {@link LogicalPlan}.
 * The {@link SchemaModel} produces the API schema. It is built incrementally while parsing the SQRL script and used
 * to resolve table and field references within the script.
 */
public class SchemaModel {

    ShadowingContainer<Table> tables;

    public static class Table implements ShadowingContainer.Nameable {

        Name name;
        ShadowingContainer<Field> fields;
        LogicalPlan.Table logicalTable;

        @Override
        public Name getName() {
            return name;
        }

        public ShadowingContainer<Field> getFields() {
            return fields;
        }

        public Optional<Table> getParentTable() {
            return fields.stream().filter(f -> f instanceof RelationshipField).map(f -> (RelationshipField)f)
                    .filter(f -> f.getType()==RelationshipType.PARENT).map(f -> f.getToTable()).findFirst();
        }
    }


    public static abstract class Field implements ShadowingContainer.Nameable {

        Name name;

        @Override
        public Name getName() {
            return name;
        }

    }

    @Getter
    public static class DataField extends Field {

        BasicType type;
        int arrayDepth;
        boolean notnull;
        LogicalPlan.Column logicalColumn;


    }

    @Getter
    public static class RelationshipField extends Field {

        Table toTable;
        RelationshipType type;
        RelationshipMultiplicity multiplicity;
        LogicalPlan.Relationship logicalRelationship;

    }

    public enum RelationshipType {
        PARENT, CHILD, JOIN;
    }

    public enum RelationshipMultiplicity {
        ZERO_ONE, ONE, MANY;
    }

}
