package ai.datasqrl.function.builtin.time;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.flink.table.planner.expressions.In;

import java.time.Instant;
import java.util.List;

public class TimestampToString {
    public String timestampToString(Instant instant) {
        return instant.toString();
    }
}
