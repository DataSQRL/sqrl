package ai.dataeng.sqml;

import static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.db.tabular.RowMapFunction;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.ingest.RecordShredder;
import ai.dataeng.sqml.ingest.SchemaAdjustmentSettings;
import ai.dataeng.sqml.ingest.SchemaValidationError;
import ai.dataeng.sqml.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.ingest.SourceTableSchema;
import ai.dataeng.sqml.ingest.SourceTableStatistics;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.source.SourceTable;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.ImportFunction;
import ai.dataeng.sqml.tree.ImportState;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Statement;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.RelationType.ImportRelationType;
import ai.dataeng.sqml.type.Type;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import lombok.Value;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DatabindContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

public class ImportPipelineResolver {

  private final Metadata metadata;

  public ImportPipelineResolver(Metadata metadata) {
    this.metadata = metadata;
  }

  public void analyze(Script script) {
    Visitor visitor = new Visitor();
    script.accept(visitor, null);
  }

  class Visitor extends AstVisitor<Void, Void> {

    @Override
    protected Void visitScript(Script node, Void context) {
      for (Node statement : node.getStatements()) {
        statement.accept(this, context);
      }
      return null;
    }

    @Override
    protected Void visitImportState(ImportState node, Void context) {
      String loc = node.getQualifiedName().getParts().get(0);
      String tableName = node.getQualifiedName().getParts().get(1);

      SourceTable stable = metadata.getDataset(loc).getTable(tableName);
      SourceTableStatistics tableStats = metadata.getDatasourceTableStatistics(stable);
      SourceTableSchema tableSchema = tableStats.getSchema();

      DataStream<SourceRecord> stream = stable.getDataStream(metadata.getFlinkEnv());
      final OutputTag<SchemaValidationError> schemaErrorTag = new OutputTag<>("schema-error-"+tableName){};
      SingleOutputStreamOperator<SourceRecord> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, tableSchema, SchemaAdjustmentSettings.DEFAULT));
      validate.getSideOutput(schemaErrorTag).addSink(new PrintSinkFunction<>()); //TODO: handle errors
      //Todo: hackery to get retail example up and running
      QualifiedName parts;
      if (tableName.equalsIgnoreCase("orders")) {
        parts = QualifiedName.of(loc, "orders", "entries");
      } else {
        parts = node.getQualifiedName();
      }

      for (int i = 1; i < parts.getParts().size(); i++) {
        //Todo: hackery, move to qualified name
        NamePath shreddingPath;
        if (i == 1) {
          shreddingPath = NamePath.ROOT;
        } else {
          List<String> shreddedPart = parts.getParts().subList(2, i + 1);
          shreddingPath = NamePath.of(shreddedPart.toArray(new String[0]));
        }
        RecordShredder shredder = RecordShredder.from(shreddingPath, tableSchema);
        SingleOutputStreamOperator<Row> process = validate.flatMap(shredder.getProcess(),
            FlinkUtilities.convert2RowTypeInfo(shredder.getResultSchema()));

        process.addSink(new PrintSinkFunction<>()); //TODO: remove, debugging only

        Table table = metadata.getStreamTableEnvironment().fromDataStream(process/*, Schema.newBuilder()
                .watermark("__timestamp", "SOURCE_WATERMARK()")
                .build()*/);
        table.printSchema();

        String shreddedTableName = tableName;
        if (shreddingPath.getNumComponents()>0) shreddedTableName += "_" + shreddingPath.toString('_');

        metadata.registerTable(QualifiedName.of(parts.getParts().subList(1, i + 1)), table, shredder.getResultSchema());
      }

      return null;
    }
  }
}
