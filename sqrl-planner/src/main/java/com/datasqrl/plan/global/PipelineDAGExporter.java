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
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.CalciteHacks;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Strings;
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
import org.apache.commons.lang3.StringUtils;

@Value
@Builder
public class PipelineDAGExporter {

  public static final String LINEBREAK = "\n";

  @Builder.Default boolean includeQueries = true;

  @Builder.Default boolean includeImports = true;

  @Builder.Default boolean withHints = false;

  @Builder.Default boolean includeLogicalPlan = true;

  @Builder.Default boolean includeSQL = true;

  // TODO: We don't yet have the physical plans in the this DAG exporter
  @Builder.Default boolean includePhysicalPlan = true;

  public List<Node> export(PipelineDAG dag) {
    CalciteHacks.resetToSqrlMetadataProvider();
    List<Node> result = new ArrayList<>();
    for (PipelineNode node : dag) {
      List<String> inputs =
          dag.getInputs(node).stream()
              .map(PipelineNode::getId)
              .sorted()
              .collect(Collectors.toUnmodifiableList());
      var stage = node.getChosenStage().engine().getName().toLowerCase();
      if (node instanceof TableNode tableNode) {
        var table = tableNode.getTableAnalysis();
        if (table.getSourceSinkTable().isPresent()) {
          if (!includeImports) {
            continue;
          }
          var source = table.getSourceSinkTable().get();
          result.add(
              Source.builder()
                  .id(table.getIdentifier().toString())
                  .name(table.getName())
                  .type(NodeType.IMPORTS.getName())
                  .connector(source.getConnector().getOptions())
                  .stage(stage)
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
                  .inputs(inputs)
                  .plan(explain(table.getCollapsedRelnode()))
                  .sql(table.getOriginalSql())
                  .primary_key(
                      table.getPrimaryKey().isUndefined()
                          ? null
                          : table.getPrimaryKey().asList().stream()
                              .flatMap(set -> set.getIndexes().stream().sorted())
                              .map(fields::get)
                              .map(RelDataTypeField::getName)
                              .collect(Collectors.toUnmodifiableList()))
                  .timestamp(
                      timestampIdx.map(fields::get).map(RelDataTypeField::getName).orElse("-"))
                  .schema(
                      fields.stream()
                          .map(
                              field ->
                                  new SchemaColumn(
                                      field.getName(), field.getType().getFullTypeString()))
                          .collect(Collectors.toUnmodifiableList()))
                  .annotations(getAnnotations(table))
                  .build());
        }
      } else if (node instanceof ExportNode export) {
        result.add(
            Node.builder()
                .id(node.getId())
                .name(export.getSinkPath().getLast().getDisplay())
                .type(NodeType.EXPORT.getName())
                .stage(stage)
                .inputs(inputs)
                .build());
      } else if (node instanceof TableFunctionNode) {
        var fct = ((TableFunctionNode) node).getFunction();
        if (fct.getVisibility().isAccessOnly() && !includeQueries) {
          continue;
        }
        result.add(
            Query.builder()
                .id(node.getId())
                .name(fct.getFullPath().toString())
                .type(NodeType.QUERY.getName())
                .stage(stage)
                .plan(explain(fct.getFunctionAnalysis().getCollapsedRelnode()))
                .sql(fct.getFunctionAnalysis().getOriginalSql())
                .annotations(getAnnotations(fct))
                .inputs(inputs)
                .build());
      } else {
        throw new UnsupportedOperationException("Unexpected DAG node: " + node);
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
    List<Annotation> result = new ArrayList<>(getAnnotations(function.getFunctionAnalysis()));
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
    List<Annotation> result = new ArrayList<>();
    List<EngineCapability.Feature> capabilities =
        StreamUtil.filterByClass(
                tableAnalysis.getRequiredCapabilities(), EngineCapability.Feature.class)
            .collect(Collectors.toList());
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
        default -> throw new UnsupportedOperationException("Unexpected type: " + tableType);
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
    @Builder.Default List<String> inputs = List.of();
    List<Annotation> annotations;

    @Override
    public String toString() {
      return baseToString();
    }

    String baseToString() {
      var s = new StringBuilder();
      s.append("=== ").append(name).append(LINEBREAK);
      s.append("ID:     ").append(id).append(LINEBREAK);
      s.append("Type:   ").append(type).append(LINEBREAK);
      s.append("Stage:  ").append(stage).append(LINEBREAK);
      if (!inputs.isEmpty()) {
        s.append("Inputs: ").append(StringUtils.join(inputs, ", ")).append(LINEBREAK);
      }
      if (annotations != null && !annotations.isEmpty()) {
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
  public static class Source extends Node {

    Map<String, String> connector;

    String connectorString() {
      if (connector != null && connector.get(FlinkConnectorConfig.CONNECTOR_KEY) != null) {
        return "Connector: " + connector.get(FlinkConnectorConfig.CONNECTOR_KEY);
      }
      return "";
    }

    @Override
    public String toString() {
      return super.toString() + connectorString();
    }
  }

  @Getter
  @SuperBuilder
  public static class Query extends Node {

    String plan;
    String sql;

    String planToString() {
      var s = new StringBuilder();
      if (!Strings.isNullOrEmpty(plan)) {
        s.append("Plan:").append(LINEBREAK);
        s.append(plan);
        //            s.append("---------------------").append(LINEBREAK);
      }
      if (!Strings.isNullOrEmpty(sql)) {
        s.append("SQL: ").append(sql);
      }
      return s.toString();
    }

    @Override
    public String toString() {
      return super.toString() + planToString();
    }
  }

  @Getter
  @SuperBuilder
  public static class Table extends Query {

    List<String> primary_key;
    String timestamp;
    List<SchemaColumn> schema;

    @Override
    public String toString() {
      var s = new StringBuilder();
      s.append(baseToString());
      s.append("Primary Key: ")
          .append(primary_key == null ? "-" : StringUtils.join(primary_key, ", "))
          .append(LINEBREAK);
      s.append("Timestamp  : ").append(timestamp).append(LINEBREAK);
      s.append("Schema:").append(LINEBREAK);
      toListString(s, schema);
      s.append(planToString());
      return s.toString();
    }
  }

  private static void toListString(StringBuilder s, List<?> items) {
    items.forEach(i -> s.append(" - ").append(i.toString()).append(LINEBREAK));
  }

  @Value
  public static class SchemaColumn {
    String name;
    String type;

    @Override
    public String toString() {
      return name + ": " + type;
    }
  }

  @Value
  public static class Annotation {
    String name;
    String description;

    @Override
    public String toString() {
      return name + ": " + description;
    }
  }
}
