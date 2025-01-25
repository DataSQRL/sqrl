package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

/**
 * A partially parsed SQRL definition. Some elements are extracted but the
 * body of the definition is kept as a string to be passed to the Flink parser
 * for parsing and conversion.
 *
 * As such, we try to do our best to keep offsets to map errors back by preserving position.
 */
@AllArgsConstructor
@Getter
public abstract class SqrlDefinition implements SqrlStatement {

  final ParsedObject<NamePath> tableName;
  final ParsedObject<String> definitionBody;
  final AccessModifier access;
  final SqrlComments comments;

  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    String prefix = getPrefix();
    return prefix + definitionBody.get();
  }

  String getPrefix() {
    return String.format("CREATE VIEW %s AS ", tableName.get().getLast().getDisplay());
  }

  public NamePath getPath() {
    return tableName.get();
  }

  public boolean isTable() {
    return tableName.get().size()==1;
  }

  public boolean isRelationship() {
    return !isTable();
  }

  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    return definitionBody.getFileLocation().add(
        SQLStatement.removeFirstRowOffset(location, getPrefix().length()));
  }
}
