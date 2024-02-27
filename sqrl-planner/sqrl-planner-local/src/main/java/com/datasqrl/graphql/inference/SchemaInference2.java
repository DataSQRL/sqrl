//package com.datasqrl.graphql.inference;
//
//import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;
//
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.canonicalizer.ReservedName;
//import com.datasqrl.config.SerializedSqrlConfig;
//import com.datasqrl.graphql.APIConnectorManager;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutation;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutations;
//import com.datasqrl.io.tables.TableSink;
//import com.datasqrl.plan.queries.APISource;
//import graphql.language.FieldDefinition;
//import graphql.language.ObjectTypeDefinition;
//import java.util.ArrayList;
//import java.util.List;
//
//public class SchemaInference2 {
//
//
//  InferredMutations visitMutation(ObjectTypeDefinition mutation, Strategy strategy) {
//    List<InferredMutation> mutations = new ArrayList<>();
//    for(FieldDefinition fieldDefinition : mutation.getFieldDefinitions()) {
//
//      InferredMutation inferredMutation = new InferredMutation(fieldDefinition.getName(), config);
//    }
//
//    return new InferredMutations(mutations);
//    //
//  }
//
//  InferredMutation visit(FieldDefinition definition, Strategy strategy) {
//    return strategy.accept(definition, null);
//  }
//
//  public interface Strategy {
//
//    InferredMutation accept(FieldDefinition fieldDefinition, Object o);
//  }
//
//  public class MutationStrategy implements Strategy {
//    private final APIConnectorManager apiManager = null;
//    private final APISource source = null;
//
//    @Override
//    public InferredMutation accept(FieldDefinition fieldDefinition, Object o) {
////      validateStructurallyEqualMutation(fieldDefinition, getValidMutationReturnType(fieldDefinition), getValidMutationInput(fieldDefinition),
////          List.of(ReservedName.SOURCE_TIME.getCanonical()));
//      TableSink tableSink = apiManager.getMutationSource(source, Name.system(fieldDefinition.getName()));
//      checkState(tableSink != null, fieldDefinition.getSourceLocation(),
//          "Could not find mutation source: %s.", fieldDefinition.getName());
//
//      //TODO: validate that tableSink schema matches Input type
//      SerializedSqrlConfig config = tableSink.getConfiguration().getConfig().serialize();
//      InferredMutation inferredMutation = new InferredMutation(fieldDefinition.getName(), config);
//      return inferredMutation;
//    }
//  }
//}
