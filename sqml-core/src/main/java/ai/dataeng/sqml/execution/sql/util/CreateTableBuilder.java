package ai.dataeng.sqml.execution.sql.util;

import static ai.dataeng.sqml.execution.sql.util.DatabaseUtil.sqlName;

import ai.dataeng.sqml.execution.sql.util.DatabaseUtil.SQLTypeMapping;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.type.basic.BooleanType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.NumberType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.type.basic.UuidType;
import com.google.common.base.Preconditions;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimestampType;

public class CreateTableBuilder extends TableBuilder {

    private final StringBuilder sql = new StringBuilder();
    private List<String> primaryKeys = new ArrayList<>();

    public CreateTableBuilder(String tableName) {
        super(tableName);
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
    }

    public CreateTableBuilder addColumns(Schema schema, Table table) {
//        for (Column pk : table.getPrimaryKeys()) {
//            primaryKeys.add(pk.getId().toString());
//        }

        List<String> columns = new ArrayList<>();
        for (UnresolvedColumn col : schema.getColumns()) {
            String column = addColumn((UnresolvedPhysicalColumn)col);
            columns.add(column);
        }

        sql.append(String.join(", ", columns));

        return this;
    }

    public String addColumn(UnresolvedPhysicalColumn column) {
        AtomicDataType type = (AtomicDataType)column.getDataType();

        return addColumn(column.getName().toString(), getSQLType(type), !type.getLogicalType().isNullable());
    }

    private String getSQLType(AtomicDataType type) {
        switch (type.getLogicalType().getTypeRoot()) {
            case BIGINT:
            case INTEGER:
                return "BIGINT";
            case CHAR:
            case VARCHAR:
                return "VARCHAR";
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
//        Preconditions.checkArgument(!primaryKeys.isEmpty());
//        if (!primaryKeys.isEmpty()) {
//            sql.append("PRIMARY KEY (");
//            sql.append(primaryKeys.stream().collect(Collectors.joining(", ")));
//            sql.append(")");
//        }
        sql.append(" );");
        System.out.println(sql.toString());
        return sql.toString();
    }
}