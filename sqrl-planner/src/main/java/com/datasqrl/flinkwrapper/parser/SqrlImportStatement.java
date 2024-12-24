package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.flinkwrapper.SqrlEnvironment;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

@Value
public class SqrlImportStatement implements SqrlStatement {

  ParsedObject<NamePath> packageIdentifier;
  ParsedObject<NamePath> alias;

  @Override
  public void apply(SqrlEnvironment sqrlEnv) {
    throw new UnsupportedOperationException();
  }
}
