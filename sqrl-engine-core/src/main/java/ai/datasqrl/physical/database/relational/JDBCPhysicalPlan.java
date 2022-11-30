package ai.datasqrl.physical.database.relational;

import ai.datasqrl.physical.database.DatabasePhysicalPlan;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.physical.database.relational.ddl.SqlDDLStatement;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class JDBCPhysicalPlan implements DatabasePhysicalPlan {

    List<SqlDDLStatement> ddlStatements;
    Map<APIQuery, QueryTemplate> queries;

}
