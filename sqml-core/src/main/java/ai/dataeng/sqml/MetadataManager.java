package ai.dataeng.sqml;

import ai.dataeng.sqml.StubModel.ModelRelation;
import ai.dataeng.sqml.common.CatalogSchemaName;
import ai.dataeng.sqml.common.ColumnHandle;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.common.type.TypeSignature;
import ai.dataeng.sqml.connector.Source;
import ai.dataeng.sqml.metadata.FunctionAndTypeManager;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.PostgresMetadataResolver;
import ai.dataeng.sqml.metadata.TableMetadata;
import ai.dataeng.sqml.relation.TableHandle;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MetadataManager implements Metadata {
  private final Session session;
  private final PostgresMetadataResolver resolver;
  public Set<MetadataColumn> seen = new HashSet<>();

  public MetadataManager(Session session, PostgresMetadataResolver resolver) {
    this.session = session;
    this.resolver = resolver;
  }

  @Override
  public Type getType(TypeSignature signature) {
    return null;
  }

  @Override
  public boolean schemaExists(Session session, CatalogSchemaName schema) {
    return false;
  }

  @Override
  public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName) {
    return resolver.getTableHandle(tableName);
  }

  @Override
  public boolean isLegacyGetLayoutSupported(Session session, TableHandle tableHandle) {
    return false;
  }

  @Override
  public TableMetadata getTableMetadata(Session session, TableHandle tableHandle) {
    return resolver.getTableMetadata(tableHandle);
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle) {
    //Could be sqml model or table
    return Map.of();
  }

  @Override
  public Optional<Object> getCatalogHandle(Session session, String catalogName) {
    return Optional.empty();
  }

  @Override
  public FunctionAndTypeManager getFunctionAndTypeManager() {
    return new FunctionAndTypeManager();
  }

  @Override
  public Connection getConnection(Session session) {
    return session.getConnection();
  }

  @Override
  public Optional<TableHandle> getSourceTable(Source source) {
    return resolver.getSourceTable(source);
  }

  @Override
  public void notifyColumn(String translatedName, String name, String type) {
    int size = seen.size();
    seen.add(new MetadataColumn(translatedName, name, type));
    if (size != seen.size()) {
      notifyNewColumn();
    }
  }

  private void notifyNewColumn() {
    resolver.notifyNewColumns(seen);
  }

  @Override
  public void createSourceTable(Source source) {
    try {
      //todo Allow table mapping
      session.getConnection().createStatement().execute(
          String.format("CREATE TABLE %s (\n"
          + "    json        jsonb"
          + ");", source.getName()));
    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
  }

  @Override
  public void createView(ModelRelation rel) {
    resolver.createView(rel);
  }

  @Override
  public Optional<TableHandle> getRelationHandle(Session session, QualifiedObjectName name) {
    return Optional.of(new TableHandle("stub"));
  }

  @Override
  public ColumnHandle getColumnHandle(Session session, String name) {
    return new ColumnHandle() {
    };
  }

  //Todo: Remove this
  public static class MetadataColumn {
    private final String translatedName;
    private final String name;
    private final String type;

    public MetadataColumn(String translatedName, String name, String type) {

      this.translatedName = translatedName;
      this.name = name;
      this.type = type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MetadataColumn that = (MetadataColumn) o;
      return translatedName.equals(that.translatedName) && name.equals(that.name) && type
          .equals(that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(translatedName, name, type);
    }

    public String getTranslatedName() {
      return translatedName;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }
}
