package ai.datasqrl.sql.ddl;

import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;

public class CreateTableBuilder extends TableBuilder {

    private final StringBuilder sql = new StringBuilder();
    private List<String> primaryKeys = new ArrayList<>();
    private Schema schema = null;

    public CreateTableBuilder(String tableName) {
        super(tableName);
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
    }

    public CreateTableBuilder addColumns(Schema schema, Table table) {
        this.schema = schema;
        if (schema.getPrimaryKey().isPresent()) {
            UnresolvedPrimaryKey key = schema.getPrimaryKey().get();
            primaryKeys = key.getColumnNames();
        } else { //todo bag logic
            for (Column pk : table.getPrimaryKeys()) {
                primaryKeys.add(pk.getId().toString());
            }
        }

        List<String> columns = new ArrayList<>();
        for (UnresolvedColumn col : schema.getColumns()) {
            String column = addColumn((UnresolvedPhysicalColumn)col);
            if (column != null) {
                columns.add(column);
            }
        }

        sql.append(String.join(", ", columns));

        return this;
    }

    public String addColumn(UnresolvedPhysicalColumn column) {
        if (column.getDataType() instanceof CollectionDataType) {

            return null;
        }

        AtomicDataType type = (AtomicDataType)column.getDataType();

        return addColumn(column.getName().toString(), getSQLType(type), !type.getLogicalType().isNullable());
    }

    private String getSQLType(AtomicDataType type) {
        switch (type.getLogicalType().getTypeRoot()) {
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case INTEGER:
                return "BIGINT";
            case CHAR:
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
            case DOUBLE:
                return "FLOAT";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMPTZ";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMPTZ";
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private String addColumn(String name, String sqlType, boolean isNonNull) {
        StringBuilder sql = new StringBuilder();
//        name = sqlName(name);
        sql.append(name).append(" ").append(sqlType).append(" ");
        if (isNonNull) sql.append("NOT NULL");
        return sql.toString();
//        if (isPrimaryKey) primaryKeys.add(name);
    }

    @Override
    public String getSQL() {
//        checkUpdate();
//        addColumn(DatabaseUtil.TIMESTAMP_COLUMN_NAME, DatabaseUtil.TIMESTAMP_COLUMN_SQL_TYPE.getTypeName(), true);

        Preconditions.checkArgument(!primaryKeys.isEmpty());
        sql.append(", PRIMARY KEY (");
        sql.append(primaryKeys.stream().collect(Collectors.joining(", ")));
        sql.append(")");
        sql.append(" );");
        System.out.println(sql.toString());
        return sql.toString();
    }
}