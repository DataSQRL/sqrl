package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.APIConnectorManagerImpl.getLogId;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.LogEngineSupplier;
import com.datasqrl.engine.log.Log;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.GraphqlSchemaParser;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSourceSinkNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Slf4j
@AllArgsConstructor(onConstructor_=@Inject)
@Getter
public class GraphQLMutationExtraction {
  private final GraphqlSchemaParser schemaParser;
  private final RelDataTypeFactory typeFactory;
  private final NameCanonicalizer canonicalizer;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errors;
  private final APIConnectorManager connectorManager;

  public void analyze(APISource apiSource) {
    TypeDefinitionRegistry registry = schemaParser.parse(apiSource.getSchemaDefinition());
    ObjectTypeDefinition mutationType = (ObjectTypeDefinition) registry
        .getType(getMutationTypeName(registry))
        .orElse(null);

    if (mutationType == null) {
      log.trace("No mutations in {}", apiSource);
    } else {
      List<NamedRelDataType> types = GraphqlSchemaVisitor.accept(
          new InputFieldToRelDataType(registry, typeFactory, canonicalizer),
          mutationType, registry);

      for (NamedRelDataType namedType : types) {
        APIMutation apiMutation = new APIMutation(namedType.getName(), apiSource,
            namedType.getType());
        connectorManager.addMutation(apiMutation);
      }
    }
  }
}
