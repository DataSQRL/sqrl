/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.plan.global;

import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.database.relational.AbstractJDBCDatabaseEngine;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.util.RelWriterWithHints;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.DAGPlanner;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.PlannedNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.FlinkConnectorConfigWrapper;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.CalciteHacks;
import com.datasqrl.util.StreamUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Value
@Builder
public class PipelineDAGExporter {

  private static final String LINEBREAK = "\n";
  private static final String N_A = "-";
  private static final String DEFINITION_SEPARATOR = "--- definition ---";
  private static final String PHYSICAL_SEPARATOR = "--- physical ---";

  @Builder.Default boolean includeQueries = true;

  @Builder.Default boolean includeImports = true;

  @Builder.Default boolean withHints = false;

  @Builder.Default boolean includeLogicalPlan = true;

  @Builder.Default boolean includeSQL = true;

  @Builder.Default boolean includeDocumentation = false;

  // TODO: We don't yet have the physical plans in the this DAG exporter
  @Builder.Default boolean includePhysicalPlan = true;

  /** The physical plan to read the selected database indexes from */
  @NonNull PhysicalPlan physicalPlan;

  public List<Node> export(PipelineDAG dag) {
    CalciteHacks.resetToSqrlMetadataProvider();
    var databaseTables = getDatabaseTables();
    var result = new ArrayList<Node>();
    for (PipelineNode node : dag) {
      var inputs = dag.getInputs(node).stream().map(PipelineNode::getId).sorted().toList();
      var stage = node.getChosenStage().engine().getName().toLowerCase();
      if (node instanceof TableNode tableNode) {
        var table = tableNode.getTableAnalysis();
        if (table.getSourceSinkTable().isPresent()) {
          if (!includeImports) {
            continue;
          }
          var source = table.getSourceSinkTable().get();
          result.add(
              SourceOrSink.builder()
                  .id(table.getIdentifier().toString())
                  .name(table.getName())
                  .type(NodeType.IMPORTS.getName())
                  .connector(source.connectorConfig().options())
                  .stage(stage)
                  .documentation(getDocumentation(tableNode))
                  .build());
        } else {
          var fields = table.getRowType().getFieldList();
          result.add(
              Table.builder()
                  .id(table.getObjectIdentifier().asSummaryString())
                  .name(table.getName())
                  .type(NodeType.from(table.getType()).getName())
                  .stage(stage)
                  .documentation(getDocumentation(tableNode))
                  .inputs(inputs)
                  .plan(explain(table.getCollapsedRelnode()))
                  .sql(getSql(table))
                  .primaryKey(
                      table.getPrimaryKey().isUndefined()
                          ? null
                          : table.getPrimaryKey().asList().stream()
                              .flatMap(set -> set.getIndexes().stream().sorted())
                              .map(fields::get)
                              .map(RelDataTypeField::getName)
                              .toList())
                  .rowCount(getRowCount(table).orElse(N_A))
                  .timestamp(
                      table
                          .getRowTime()
                          .map(fields::get)
                          .map(RelDataTypeField::getName)
                          .orElse(N_A))
                  .schema(
                      fields.stream()
                          .map(
                              field ->
                                  new SchemaColumn(
                                      field.getName(),
                                      field.getType().getFullTypeString(),
                                      table.getDocumentation().getColumn(field.getName(), null)))
                          .toList())
                  .annotations(getAnnotations(table))
                  .databaseTables(databaseTables.get(table.getObjectIdentifier()))
                  .build());
        }
      } else if (node instanceof ExportNode export) {
        result.add(
            SourceOrSink.builder()
                .id(node.getId())
                .name(export.getSinkPath().getLast().getDisplay())
                .type(NodeType.EXPORT.getName())
                .stage(stage)
                .inputs(inputs)
                .connector(export.getConnectorConfig().orElse(Map.of()))
                .build());
      } else if (node instanceof TableFunctionNode fctNode) {
        var fct = fctNode.getFunction();
        if (fct.getVisibility().isAccessOnly() && !includeQueries) {
          continue;
        }
        result.add(
            Query.builder()
                .id(node.getId())
                .name(fct.getFullPath().toString())
                .type(NodeType.QUERY.getName())
                .stage(stage)
                .documentation(getDocumentation(fctNode))
                .plan(explain(fct.getFunctionAnalysis().getCollapsedRelnode()))
                .sql(getSql(fct.getFunctionAnalysis()))
                .annotations(getAnnotations(fct))
                .inputs(inputs)
                .build());
      } else {
        throw new UnsupportedOperationException("Unexpected DAG node: " + node);
      }
    }
    return result;
  }

  private String getDocumentation(PlannedNode node) {
    if (includeDocumentation) {
      return node.getDocumentation().getDocString(null);
    }

    return null;
  }

  private String getSql(TableAnalysis tableAnalysis) {
    if (includeSQL) {
      return tableAnalysis.getOriginalSql();
    }

    return null;
  }

  private Optional<String> getRowCount(TableAnalysis tableAnalysis) {
    var stats = tableAnalysis.getTableStatistic();
    if (stats.isUnknown()) {
      return Optional.empty();
    }

    var rowCount = stats.getRowCount();
    String rowCountStr;
    if (rowCount < 1) {
      rowCountStr = "1";

    } else if (rowCount < 10) {
      rowCountStr = String.valueOf((int) rowCount);

    } else {
      rowCountStr = new DecimalFormat("0E0").format(rowCount).toLowerCase();
    }

    if (stats.isEstimated()) {
      rowCountStr = "~" + rowCountStr;
    }

    return Optional.of(rowCountStr);
  }

  /**
   * Collects the database tables with their primary key and indexes, keyed by the DAG table that
   * each database table stores, since the database table names differ from the DAG.
   */
  private Map<ObjectIdentifier, List<DatabaseTable>> getDatabaseTables() {
    var result = new HashMap<ObjectIdentifier, List<DatabaseTable>>();
    for (var plan : physicalPlan.getPlans(JdbcPhysicalPlan.class).toList()) {
      var stage = plan.stage().engine().getName().toLowerCase();
      var hasPrimaryKeyIndex =
          plan.stage().engine() instanceof AbstractJDBCDatabaseEngine engine
              && engine.getIndexSelectorConfig().hasPrimaryKeyIndex();
      var indexesByTable = plan.getIndexesByTable();

      for (var table : plan.tableIdMap().values()) {
        var indexes = new ArrayList<TableIndex>();
        if (hasPrimaryKeyIndex && !table.getPrimaryKey().isEmpty()) {
          indexes.add(TableIndex.primaryKey(table.getPrimaryKey()));
        }

        indexesByTable.getOrDefault(table.getName(), List.of()).stream()
            .sorted()
            .map(TableIndex::of)
            .forEach(indexes::add);

        if (!indexes.isEmpty()) {
          // A DAG table can be stored in more than one database table, e.g. when also exported
          result
              .computeIfAbsent(
                  table.getEngineTable().tableAnalysis().getObjectIdentifier(),
                  id -> new ArrayList<>())
              .add(new DatabaseTable(stage, table.getName(), indexes));
        }
      }
    }
    return result;
  }

  private String explain(RelNode relNode) {
    if (!includeLogicalPlan) {
      return null;
    }
    CalciteHacks.resetToSqrlMetadataProvider();
    if (withHints) {
      return RelWriterWithHints.explain(relNode);
    } else {
      return relNode.explain();
    }
  }

  private static List<Annotation> getAnnotations(SqrlTableFunction function) {
    var result = new ArrayList<>(getAnnotations(function.getFunctionAnalysis()));

    if (!function.getParameters().isEmpty()) {
      result.add(
          new Annotation(
              "parameters",
              function.getParameters().stream()
                  .map(FunctionParameter::getName)
                  .collect(Collectors.joining(", "))));
    }

    result.add(new Annotation("base-table", function.getBaseTable().getName()));

    return result;
  }

  private static List<Annotation> getAnnotations(TableAnalysis tableAnalysis) {
    var result = new ArrayList<Annotation>();
    var capabilities =
        StreamUtil.filterByClass(
                tableAnalysis.getRequiredCapabilities(), EngineCapability.Feature.class)
            .toList();

    if (!capabilities.isEmpty()) {
      result.add(
          new Annotation(
              "features",
              capabilities.stream()
                  .map(EngineCapability::getName)
                  .collect(Collectors.joining(", "))));
    }

    if (tableAnalysis.isMostRecentDistinct()) {
      result.add(new Annotation("mostRecentDistinct", "true"));
    }

    if (tableAnalysis.getStreamRoot().isPresent()) {
      result.add(new Annotation("stream-root", tableAnalysis.getStreamRoot().get().getName()));
    }

    if (tableAnalysis.getTopLevelSort().isPresent()) {
      var sort = tableAnalysis.getTopLevelSort().get();
      result.add(new Annotation("sort", sort.getCollation().toString()));
    }

    return result;
  }

  @AllArgsConstructor
  @Getter
  public enum NodeType {
    STREAM("stream"),
    STATE("state"),
    RELATION("relation"),
    QUERY("query"),
    EXPORT("export"),
    IMPORTS("import");

    private final String name;

    @Override
    public String toString() {
      return name;
    }

    public static NodeType from(TableType tableType) {
      return switch (tableType) {
        case RELATION -> RELATION;
        case STREAM -> STREAM;
        case STATE, STATIC, LOOKUP, VERSIONED_STATE -> STATE;
      };
    }
  }

  /**
   * Generic DAG node that is the base for all specific node types and used for imports and exports
   */
  @Getter
  @SuperBuilder
  public static class Node implements Comparable<Node> {

    String id;
    String name;
    String type;
    String stage;
    String documentation;
    List<String> inputs;
    List<Annotation> annotations;

    @Override
    public String toString() {
      return getBaseHeaderString() + DEFINITION_SEPARATOR + LINEBREAK + getBaseListsString();
    }

    String getBaseHeaderString() {
      var header =
          "=== "
              + name
              + LINEBREAK
              + "ID:          "
              + id
              + LINEBREAK
              + "Type:        "
              + type
              + LINEBREAK
              + "Stage:       "
              + stage
              + LINEBREAK;

      if (StringUtils.isNotBlank(documentation)) {
        header += "Docs:       " + documentation + LINEBREAK;
      }

      return header;
    }

    String getBaseListsString() {
      var s = new StringBuilder();
      if (CollectionUtils.isNotEmpty(inputs)) {
        s.append("Inputs:").append(LINEBREAK);
        toListString(s, inputs);
      }

      if (CollectionUtils.isNotEmpty(annotations)) {
        s.append("Annotations:").append(LINEBREAK);
        toListString(s, annotations);
      }

      return s.toString();
    }

    @Override
    public int compareTo(PipelineDAGExporter.Node other) {
      return this.getId().compareTo(other.getId());
    }
  }

  @Getter
  @SuperBuilder
  public static class SourceOrSink extends Node {

    Map<String, String> connector;

    String connectorString() {
      if (connector != null && connector.get(FlinkConnectorConfigWrapper.CONNECTOR_KEY) != null) {
        return "Connector:   "
            + connector.get(FlinkConnectorConfigWrapper.CONNECTOR_KEY)
            + LINEBREAK;
      }
      return "";
    }

    @Override
    public String toString() {
      return getBaseHeaderString()
          + connectorString()
          + DEFINITION_SEPARATOR
          + LINEBREAK
          + getBaseListsString();
    }
  }

  @Getter
  @SuperBuilder
  public static class Query extends Node {

    String plan;
    String sql;

    String getPlanString() {
      var s = new StringBuilder();

      if (StringUtils.isNotBlank(plan)) {
        s.append("Plan:").append(LINEBREAK).append(plan);
      }

      if (StringUtils.isNotBlank(sql)) {
        s.append("SQL:").append(LINEBREAK).append(sql);
      }

      return s.toString();
    }

    @Override
    public String toString() {
      return super.toString() + getPlanString();
    }
  }

  @Getter
  @SuperBuilder
  public static class Table extends Query {

    @JsonProperty("primary_key")
    List<String> primaryKey;

    @JsonProperty("row_count")
    String rowCount;

    String timestamp;
    List<SchemaColumn> schema;

    @JsonProperty("database_tables")
    List<DatabaseTable> databaseTables;

    @Override
    public String toString() {
      var s = new StringBuilder();
      var pKey = CollectionUtils.isEmpty(primaryKey) ? N_A : String.join(", ", primaryKey);

      s.append(getBaseHeaderString());
      s.append("Primary key: ").append(pKey).append(LINEBREAK);
      s.append("Timestamp:   ").append(timestamp).append(LINEBREAK);
      s.append("Row count:   ").append(rowCount).append(LINEBREAK);
      s.append(DEFINITION_SEPARATOR).append(LINEBREAK);
      s.append("Schema:").append(LINEBREAK);
      toListString(s, schema);
      s.append(getBaseListsString());
      s.append(getPlanString());
      if (CollectionUtils.isNotEmpty(databaseTables)) {
        // The SQL of the logical representation does not always end with a line break
        if (!StringUtils.endsWith(s, LINEBREAK)) {
          s.append(LINEBREAK);
        }
        s.append(getPhysicalString());
      }

      return s.toString();
    }

    /** The physical representation follows the logical one, after a separator of its own */
    String getPhysicalString() {
      var s = new StringBuilder(PHYSICAL_SEPARATOR).append(LINEBREAK);
      for (var table : databaseTables) {
        s.append("Table %s (%s):".formatted(table.name(), table.stage())).append(LINEBREAK);
        toListString(s, table.indexes());
      }
      return s.toString();
    }
  }

  private static void toListString(StringBuilder s, List<?> items) {
    items.forEach(i -> s.append(" - ").append(i.toString()).append(LINEBREAK));
  }

  private record SchemaColumn(String name, String type, String description) {

    @Override
    public String toString() {
      return name + ": " + type;
    }
  }

  private record Annotation(String name, String description) {

    @Override
    public String toString() {
      return name + ": " + description;
    }
  }

  /** A database table that stores a DAG table, with its primary key and indexes */
  private record DatabaseTable(String stage, String name, List<TableIndex> indexes) {}

  /**
   * An index of a database table. The primary key has no name of its own. The columns use the
   * syntax of the {@code index} hint, e.g. {@code time DESC}.
   */
  private record TableIndex(String name, String type, List<String> columns) {

    static TableIndex primaryKey(List<String> columns) {
      return new TableIndex(null, "primary key", columns);
    }

    static TableIndex of(IndexDefinition index) {
      var columns =
          IntStream.range(0, index.getColumnNames().size())
              .mapToObj(
                  i ->
                      index.getColumnNames().get(i)
                          + (index.getDirections().get(i).isDescending() ? " DESC" : ""))
              .toList();

      return new TableIndex(index.getName(), index.getType().name().toLowerCase(), columns);
    }

    @Override
    public String toString() {
      var label = name == null ? type : "index %s (%s)".formatted(name, type);
      return label
          + ": "
          + columns.stream().map(TableIndex::describeColumn).collect(Collectors.joining(", "));
    }

    // The synthetic hash key is not part of the table schema, so it needs an explanation
    private static String describeColumn(String column) {
      return column.equalsIgnoreCase(DAGPlanner.HASHED_PK_NAME)
          ? column + " (generated hash key)"
          : column;
    }
  }
}
