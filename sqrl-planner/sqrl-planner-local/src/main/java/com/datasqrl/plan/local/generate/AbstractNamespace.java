package com.datasqrl.plan.local.generate;

import java.util.List;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlOperatorTable;

@Deprecated
public interface AbstractNamespace {

  SqlOperatorTable getOperatorTable();

  SqrlSchema getSchema();

  List<ResolvedExport> getExports();

  java.util.Set<java.net.URL> getJars();
}
