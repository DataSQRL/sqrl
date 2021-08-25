package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.db.tabular.RowMapFunction;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.IntegerType;
import ai.dataeng.sqml.type.RelationType.ImportRelationType;
import ai.dataeng.sqml.type.RelationType.NamedRelationType;
import ai.dataeng.sqml.type.RelationType.RootRelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import ai.dataeng.sqml.type.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.Table;

public class FlinkViewBuilder {

  private final Analysis analysis;
  private final Metadata metadata;

  public FlinkViewBuilder(Analysis analysis, Metadata metadata) {

    this.analysis = analysis;
    this.metadata = metadata;
  }

  public void build(){
    Visitor visitor = new Visitor();
    analysis.getModel().accept(visitor, null);
  }

  class Context {

  }

  class Visitor extends SqmlTypeVisitor<Void, Context> {
    Map<String, Table> shreddedImports = new HashMap<>();

    @Override
    public Void visitSqmlType(Type type, Context context) {
      throw new RuntimeException("Unknown type " + type);
    }

    @Override
    public Void visitInteger(IntegerType type, Context context) {
      return super.visitInteger(type, context);
    }

    @Override
    public Void visitNamedRelation(NamedRelationType type, Context context) {


      return super.visitNamedRelation(type, context);
    }

    @Override
    public Void visitRootRelation(RootRelationType type, Context context) {

      for (Field field : type.getFields()) {
        field.getType().accept(this, context);
      }

      return null;
    }

    @Override
    public Void visitImportRelation(ImportRelationType type, Context context) {

      return super.visitImportRelation(type, context);
    }
  }
}
