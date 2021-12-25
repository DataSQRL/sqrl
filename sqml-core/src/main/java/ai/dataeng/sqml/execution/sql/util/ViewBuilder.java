package ai.dataeng.sqml.execution.sql.util;

import java.util.List;

public class ViewBuilder extends TableBuilder {

    private ViewBuilder(String tableName, List<String> dmlQueries, DatabaseUtil dbUtil) {
        super(tableName, dmlQueries, dbUtil);
    }

    @Override
    public String getSQL() {
        return null; //TODO
    }
}
