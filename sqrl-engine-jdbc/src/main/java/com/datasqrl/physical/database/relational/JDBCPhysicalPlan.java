package com.datasqrl.physical.database.relational;

import com.datasqrl.physical.database.DatabasePhysicalPlan;
import com.datasqrl.physical.database.QueryTemplate;
import com.datasqrl.physical.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.plan.queries.APIQuery;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class JDBCPhysicalPlan implements DatabasePhysicalPlan {

    List<SqlDDLStatement> ddlStatements;
    Map<APIQuery, QueryTemplate> queries;

}
