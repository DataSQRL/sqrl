package ai.datasqrl.graphql.inference.argument;

import ai.datasqrl.graphql.inference.ArgumentSet;
import ai.datasqrl.graphql.server.Model.PgParameterHandler;
import ai.datasqrl.schema.SQRLTable;
import graphql.language.InputValueDefinition;
import java.util.List;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.tools.RelBuilder;

@Value
public class ArgumentHandlerContextV1 {

  InputValueDefinition arg;
  Set<ArgumentSet> argumentSet;
  SQRLTable table;
  RelBuilder relBuilder;
  List<PgParameterHandler> sourceHandlers;
}