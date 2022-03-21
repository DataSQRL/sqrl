package ai.dataeng.sqml.execution.sql.util;

import static ai.dataeng.sqml.execution.sql.util.DatabaseUtil.sqlName;

import ai.dataeng.sqml.planner.Column;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CreateTableBuilder extends TableBuilder {

    private final StringBuilder sql = new StringBuilder();
    private List<String> primaryKeys = new ArrayList<>();

    public CreateTableBuilder(String tableName, List<String> dmlQueries, DatabaseUtil dbUtil) {
        super(tableName, dmlQueries, dbUtil);
        sql.append("CREATE TABLE IF NOT EXISTS ").append(sqlName(tableName)).append(" (");
    }

    public CreateTableBuilder addColumns(Column[] columns) {
        for (Column col : columns) addColumn(col);
        return this;
    }

    public CreateTableBuilder addColumn(Column column) {
        addColumn(column.getId(), dbUtil.getSQLType(column).getTypeName(), column.isNonNull(), column.isPrimaryKey());
        return this;
    }

    private void addColumn(String name, String sqlType, boolean isNonNull, boolean isPrimaryKey) {
        name = sqlName(name);
        sql.append(name).append(" ").append(sqlType).append(" ");
        if (isNonNull) sql.append("NOT NULL");
        sql.append(",");
        if (isPrimaryKey) primaryKeys.add(name);
    }

    @Override
    String getSQL() {
        checkUpdate();
        addColumn(DatabaseUtil.TIMESTAMP_COLUMN_NAME, DatabaseUtil.TIMESTAMP_COLUMN_SQL_TYPE.getTypeName(), true, false);
        Preconditions.checkArgument(!primaryKeys.isEmpty());
        sql.append("PRIMARY KEY (");
        sql.append(primaryKeys.stream().collect(Collectors.joining(", ")));
        sql.append(") );");
        return sql.toString();
    }
}