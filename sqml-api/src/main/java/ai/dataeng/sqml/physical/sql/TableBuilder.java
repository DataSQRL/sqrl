package ai.dataeng.sqml.physical.sql;

import com.google.common.base.Preconditions;

import java.util.List;

abstract class TableBuilder {

    final String tableName;
    boolean isFinished;

    final List<String> dmlQueries;
    final DatabaseUtil dbUtil;

    TableBuilder(String tableName, List<String> dmlQueries, DatabaseUtil dbUtil) {
        this.tableName = tableName;
        this.dmlQueries = dmlQueries;
        this.dbUtil = dbUtil;
    }

    void checkUpdate() {
        Preconditions.checkArgument(!isFinished);
    }

    abstract String getSQL();

    public String finish() {
        if (!isFinished) {
            dmlQueries.add(getSQL());
            isFinished = true;
        }
        return tableName;
    }

}
