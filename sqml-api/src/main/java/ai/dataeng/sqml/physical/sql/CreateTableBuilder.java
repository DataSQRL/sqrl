package ai.dataeng.sqml.physical.sql;

import ai.dataeng.sqml.logical4.LogicalPlan;
import com.google.common.base.Preconditions;

import static ai.dataeng.sqml.physical.sql.DatabaseUtil.sqlName;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CreateTableBuilder extends TableBuilder {

    private final StringBuilder sql = new StringBuilder();
    private List<String> primaryKeys = new ArrayList<>();

    public CreateTableBuilder(String tableName, List<String> dmlQueries, DatabaseUtil dbUtil) {
        super(tableName, dmlQueries, dbUtil);
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(sqlName(tableName)).append(" (");
    }

    public void addColumn(LogicalPlan.Column)

    @Override
    String getSQL() {
        checkUpdate();
        Preconditions.checkArgument(!primaryKeys.isEmpty());
        sql.append("PRIMARY KEY (");
        sql.append(primaryKeys.stream().collect(Collectors.joining(", ")));
        sql.append(") );");
        return sql.toString();
    }
}


/*
    public String createTableDML(String tableName, LogicalPlan.Column[] schema) {
        DSLContext context = DSL.using(configuration.getDialect());
        CreateTableColumnStep createTable = context.createTableIfNotExists(tableName);
        for (LogicalPlan.Column column : schema) {
            createTable = createTable.column(column.getId(), getJooqSQLType(column));
        }
        createTable = createTable.column(TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_JOOQ_TYPE);
        CreateTableConstraintStep tableConstraint = createTable.primaryKey(getPrimaryKeyNames(schema).toArray(String[]::new));
        return tableConstraint.getSQL();
    }
 */