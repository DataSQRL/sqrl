package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.plan.queries.APIQuery;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class JDBCPhysicalPlan implements DatabasePhysicalPlan {

    List<SqlDDLStatement> ddlStatements;
    Map<APIQuery, QueryTemplate> queries;

}
