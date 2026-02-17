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

import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.util.RelWriterWithHints;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.PlannedNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.CalciteHacks;
import com.datasqrl.util.StreamUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Value
@Builder
public class PipelineDAGExporter {

  private static final String LINEBREAK = "\n";
  private static final String N_A = "-";

  @Builder.Default boolean includeQueries = true;

  @Builder.Default boolean includeImports = true;

  @Builder.Default boolean withHints = false;

  @Builder.Default boolean includeLogicalPlan = true;

  @Builder.Default boolean includeSQL = true;

  @Builder.Default boolean includeDocumentation = false;

  // TODO: We don't yet have the physical plans in the this DAG exporter
  @Builder.Default boolean includePhysicalPlan = true;

  public List<Node> export(PipelineDAG dag) {
    CalciteHacks.resetToSqrlMetadataProvider();
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
                  .connector(source.connectorConfig().getOptions())
                  .stage(stage)
                  .documentation(getDocumentation(tableNode))
                  .build());
        } else {
          var fields = table.getRowType().getFieldList();
          var timestampIdx = table.getRowTime();
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
                              .flatMap(set -> set.indexes().stream().sorted())
                              .map(fields::get)
                              .map(RelDataTypeField::getName)
                              .toList())
                  .timestamp(
                      timestampIdx.map(fields::get).map(RelDataTypeField::getName).orElse(N_A))
                  .schema(
                      fields.stream()
                          .map(
                              field ->
                                  new SchemaColumn(
                                      field.getName(), field.getType().getFullTypeString()))
                          .toList())
                  .annotations(getAnnotations(table))
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
                .connector(
                    export
                        .getConnectorConfig()
                        .map(FlinkConnectorConfig::getOptions)
                        .orElse(Map.of()))
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
      return node.getDocumentation();
    } else {
      return null;
    }
  }

  private String getSql(TableAnalysis tableAnalysis) {
    if (!includeSQL) {
      return null;
    }
    return tableAnalysis.getOriginalSql();
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
      return getBaseHeaderString() + "---" + LINEBREAK + getBaseListsString();
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
      if (connector != null && connector.get(FlinkConnectorConfig.CONNECTOR_KEY) != null) {
        return "Connector:   " + connector.get(FlinkConnectorConfig.CONNECTOR_KEY) + LINEBREAK;
      }
      return "";
    }

    @Override
    public String toString() {
      return getBaseHeaderString() + connectorString() + "---" + LINEBREAK + getBaseListsString();
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

    String timestamp;
    List<SchemaColumn> schema;

    @Override
    public String toString() {
      var s = new StringBuilder();
      var pKey = CollectionUtils.isEmpty(primaryKey) ? N_A : String.join(", ", primaryKey);

      s.append(getBaseHeaderString());
      s.append("Primary key: ").append(pKey).append(LINEBREAK);
      s.append("Timestamp:   ").append(timestamp).append(LINEBREAK);
      s.append("---").append(LINEBREAK);
      s.append("Schema:").append(LINEBREAK);
      toListString(s, schema);
      s.append(getBaseListsString());
      s.append(getPlanString());

      return s.toString();
    }
  }

  private static void toListString(StringBuilder s, List<?> items) {
    items.forEach(i -> s.append(" - ").append(i.toString()).append(LINEBREAK));
  }

  private record SchemaColumn(String name, String type) {

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
}
