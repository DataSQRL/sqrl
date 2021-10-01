package ai.dataeng.sqml.analyzer;

import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.ImportFunction;
import ai.dataeng.sqml.tree.ImportData;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.RelationType.ImportRelationType;
import ai.dataeng.sqml.type.Type;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DatabindContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ImportAnalyzer {

  protected final Metadata metadata;

  public ImportAnalyzer(Metadata metadata) {
    this.metadata = metadata;
  }

  public Scope analyzeImport(Import node, Scope scope) {
    Visitor visitor = new Visitor();
    return node.accept(visitor, scope);
  }

  class Visitor extends AstVisitor<Scope, Scope> {
    private ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    @Override
    protected Scope visitImportFunction(ImportFunction node, Scope scope) {
      scope.addFunction(node.getQualifiedName());
      return scope;
    }

    @Override
    protected Scope visitImportState(ImportData node, Scope scope) {
      if (scope.resolveRelation(QualifiedName.of(node.getQualifiedName().getSuffix())).isPresent()) {
        throw new RuntimeException(String.format("Imported Field already exists %s", node.getQualifiedName()));
      }
      Path path = node.getQualifiedName().getPrefix()
          .map(p->toPath(p, metadata.getBundle().getPath()))
          .orElse(metadata.getBundle().getPath());
      Path schema = path.resolve("schema.yml");
      ImportSchema importSchema = null;
      try {
        importSchema = mapper.readValue(schema.toFile(),
            ImportSchema.class);
      } catch (IOException e) {
        throw new RuntimeException("Could not read bundle", e);
      }
      Field type = importSchema.accept(new StateVisitor(),
          new ImportContext(node.getQualifiedName().getSuffix(),
              scope, null));
      scope.addRootField(type);
      return scope;
    }

    private Path toPath(QualifiedName p, Path path) {
      for (String part : p.getParts()) {
        path = path.resolve(part);
      }
      return path;
    }
  }

  public class StateVisitor extends ImportSchemaVisitor<Field, ImportContext> {

    @Override
    public Field visitSchema(ImportSchema importSchema, ImportContext context) {
      for (ImportTable table : importSchema.tables) {
        if (table.name.equalsIgnoreCase(context.getStateName())) {
          return table.accept(this, context);
        }
      }
      return null;
    }

    @Override
    public Field visitTable(ImportTable importTable, ImportContext context) {
      return visitNestedColumn(importTable, context);
    }

    @Override
    public Field visitSource(ImportSource importSource, ImportContext context) {
      return super.visitSource(importSource, context);
    }

    @Override
    public Field visitDataColumn(ImportDataColumn importDataColumn, ImportContext context) {
      return Field.newUnqualified(importDataColumn.name, Type.fromName(importDataColumn.type));
    }

    @Override
    public Field visitNestedColumn(ImportNestedColumn importNestedColumn, ImportContext context) {
      context.name = context.getName(importNestedColumn.name);
      RelationType rel = new ImportRelationType(new RelationType(), importNestedColumn.name,
          metadata.getTableHandle(context.name));
      for (ImportColumn column : importNestedColumn.columns) {
        Field field = column.accept(this, context);
        if (field.getType() instanceof RelationType) { //set parent
          ((RelationType) field.getType()).setParent(rel);
        }
        rel.addField(field);
      }
      return Field.newUnqualified(importNestedColumn.name, rel);
    }
  }


  @AllArgsConstructor
  @Getter
  public class ImportContext {
    public String stateName;
    public Scope scope;
    public QualifiedName name;
    public QualifiedName getName(String value) {
      if (name == null) {
        return QualifiedName.of(value);
      } else {
        return name.append(value);
      }
    }
  }

  public static abstract class ImportSchemaVisitor<R, C> {
    public R visitSchema(ImportSchema importSchema, C context) {
      return null;
    }

    public R visitTable(ImportTable importTable, C context) {
      return null;
    }

    public R visitSource(ImportSource importSource, C context) {
      return null;
    }

    public R visitColumn(ImportColumn importColumn, C context) {
      return null;
    }

    public R visitDataColumn(ImportDataColumn importDataColumn, C context) {
      return null;
    }

    public R visitNestedColumn(ImportNestedColumn importNestedColumn, C context) {
      return null;
    }
  }

  public static class ImportSchema {
    public String version;
    public List<ImportTable> tables;
    public <R, C> R accept(ImportSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitSchema(this, context);
    }
  }
  public static class ImportTable extends ImportNestedColumn {
    public String description;
    public List<ImportSource> source;

    public <R, C> R accept(ImportSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitTable(this, context);
    }
  }
  public static class ImportSource {
    public String type;
    public String name;

    public <R, C> R accept(ImportSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitSource(this, context);
    }
  }

  @JsonTypeInfo(use = DEDUCTION)
  @JsonSubTypes({
      @JsonSubTypes.Type(ImportDataColumn.class),
      @JsonSubTypes.Type(ImportNestedColumn.class)
  })
  public static abstract class ImportColumn {
    public String name;
    public String description;
    public List<String> tests;
    public <R, C> R accept(ImportSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitColumn(this, context);
    }
  }
  public static class ImportDataColumn extends ImportColumn {
    public String type;
    public <R, C> R accept(ImportSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitDataColumn(this, context);
    }
  }
  public static class ImportNestedColumn extends ImportColumn {
    public List<ImportColumn> columns;

    public <R, C> R accept(ImportSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitNestedColumn(this, context);
    }
  }

  public static class ColumnTypeResolver extends TypeIdResolverBase {

    @Override
    public String idFromValue(Object o) {
      throw new RuntimeException("Serializable not implemented");
    }

    @Override
    public String idFromValueAndType(Object o, Class<?> aClass) {
      throw new RuntimeException("Serializable not implemented");
    }
    @Override
    public JavaType typeFromId(DatabindContext context, String id) throws IOException {
      if (id == null) {
        return context.getTypeFactory().constructType(new TypeReference<ImportDataColumn>() {});
      } else {
        return context.getTypeFactory().constructType(new TypeReference<ImportNestedColumn>() {});
      }
    }
    @Override
    public Id getMechanism() {
      return JsonTypeInfo.Id.CUSTOM;
    }
  }
}
