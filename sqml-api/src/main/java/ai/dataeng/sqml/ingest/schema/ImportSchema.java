package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.schema.name.Name;

import java.util.List;

public class ImportSchema {

    List<Dataset> datasets;

    public static class Dataset {

        Name name;
        List<TableField> tables;

    }

    public static class Field {

        Name name;

        boolean isArray;
        boolean notNull;

        //TODO: add hints and stats

    }

    public static class BasicField extends Field {

        ScalarType type;

    }

    public static class TableField extends Field {

        List<Field> fields;

    }

}
