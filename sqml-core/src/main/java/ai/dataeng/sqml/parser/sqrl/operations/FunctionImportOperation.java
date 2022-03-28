package ai.dataeng.sqml.parser.sqrl.operations;

import ai.dataeng.sqml.functions.ResolvedFunction;
import java.util.List;
import lombok.Value;

/**
 * Adds a function to the namespace.
 */
@Value
public class FunctionImportOperation extends ImportOperation {
  String alias;
  List<ResolvedFunction> functions;
}