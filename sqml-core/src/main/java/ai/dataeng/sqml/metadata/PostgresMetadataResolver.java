package ai.dataeng.sqml.metadata;

import ai.dataeng.sqml.MetadataManager.MetadataColumn;
import ai.dataeng.sqml.StubModel.ModelRelation;
import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.type.IntegerType;
import ai.dataeng.sqml.connector.Source;
import ai.dataeng.sqml.plan.ProjectNode;
import ai.dataeng.sqml.planner.SimplePlanVisitor;
import ai.dataeng.sqml.relation.TableHandle;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PostgresMetadataResolver {

  private String informationSchema = "select table_type, t.table_schema,\n"
      + "       t.table_name,\n"
      + "       c.column_name,\n"
      + "       c.data_type,\n"
      + "       case when c.character_maximum_length is not null\n"
      + "            then c.character_maximum_length\n"
      + "            else c.numeric_precision end as max_length,\n"
      + "       is_nullable\n"
      + "       from information_schema.tables t\n"
      + "    left join information_schema.columns c \n"
      + "              on t.table_schema = c.table_schema \n"
      + "              and t.table_name = c.table_name\n"
      + "where t.table_schema not in ('information_schema', 'pg_catalog')\n"
      + ";";

  private final Session session;
  private Map<String, TableHandle> tables;
  private Set<MetadataColumn> seen = new HashSet<>();

  public PostgresMetadataResolver(Session session) {
    this.session = session;
    this.tables = resolveTables(session);
  }

  private Map<String, TableHandle> resolveTables(Session session) {
    Map<String, TableHandle> tables = new HashMap<>();
    try {
      PreparedStatement s = session.getConnection().prepareStatement("select distinct t.table_name as table_name\n"
          + "       from information_schema.tables t\n"
          + "    left join information_schema.columns c \n"
          + "              on t.table_schema = c.table_schema \n"
          + "              and t.table_name = c.table_name\n"
          + "where t.table_schema not in ('information_schema', 'pg_catalog')\n"
          + ";");
      ResultSet resultSet = s.executeQuery();
      while (resultSet.next()) {
        String tableName = resultSet.getString(1);
        tables.put(tableName, new TableHandle(tableName));
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }

    return tables;
  }

  public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName) {
    TableHandle handle = tables.get(tableName.getObjectName());
    return Optional.ofNullable(handle);
  }

  public TableMetadata getTableMetadata(TableHandle tableHandle) {
    //Keep a collection of seen columns and query seen columns from the database (from plan?)
    List<ColumnMetadata> columnMetadata = new ArrayList<>();
    columnMetadata.add(new ColumnMetadata("_id", IntegerType.INTEGER));

    for (MetadataColumn column : seen) {
      String columnName = column.getName().split("\\.")[1];
      columnMetadata.add(new ColumnMetadata(columnName, IntegerType.INTEGER));
    }
    return new TableMetadata(columnMetadata);
  }

  public Optional<TableHandle> getSourceTable(Source source) {
    //todo: validate this is an ingest table
    //Todo: Allow a table mapping here
    return Optional.ofNullable(
        tables.get(source.getName())
    );
  }

  public void createView(ModelRelation rel) {
    ViewGeneratorVisitor visitor = new ViewGeneratorVisitor();
    StringBuilder builder = new StringBuilder();
    rel.relation.accept(visitor, builder);
    System.out.println(builder);
    String view = String.format("CREATE VIEW %s AS %s", rel.name, builder);
  }

  public void notifyNewColumns(Set<MetadataColumn> seen) {
    this.seen = seen;

  }

  //Todo: Generate view & push it to postgres
  class ViewGeneratorVisitor extends SimplePlanVisitor<StringBuilder> {

    @Override
    public Void visitProject(ProjectNode node, StringBuilder context) {
      return null;
    }
  }
}
