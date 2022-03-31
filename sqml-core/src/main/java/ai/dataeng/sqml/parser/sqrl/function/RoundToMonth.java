package ai.dataeng.sqml.parser.sqrl.function;

public class RoundToMonth implements RewritingFunction {

  @Override
  public boolean isAggregate() {
    return false;
  }
}
