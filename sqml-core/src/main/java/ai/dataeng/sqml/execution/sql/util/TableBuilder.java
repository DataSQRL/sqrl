package ai.dataeng.sqml.execution.sql.util;

import com.google.common.base.Preconditions;
import java.util.List;

public abstract class TableBuilder {

    final String tableName;
    boolean isFinished;

    TableBuilder(String tableName) {
        this.tableName = tableName;
    }
//
//    void checkUpdate() {
//        Preconditions.checkArgument(!isFinished);
//    }

    abstract String getSQL();
}
