package com.datasqrl.functions.time;

import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.google.common.base.Preconditions;
import java.time.temporal.ChronoUnit;
import lombok.AllArgsConstructor;

public abstract class AbstractEndOfMetadata implements SqrlTimeTumbleFunction {

  protected final ChronoUnit timeUnit;
  protected final ChronoUnit offsetUnit;

  public AbstractEndOfMetadata(ChronoUnit timeUnit, ChronoUnit offsetUnit) {
    this.timeUnit = timeUnit;
    this.offsetUnit = offsetUnit;
  }

  @Override
  public Specification getSpecification(long[] arguments) {
    Preconditions.checkArgument(arguments != null);
    return new Specification(arguments.length > 0 ? arguments[0] : 1,
        arguments.length > 1 ? arguments[1] : 0);
  }


  @AllArgsConstructor
  private class Specification implements SqrlTimeTumbleFunction.Specification {

    final long widthMultiple;
    final long offsetMultiple;

    @Override
    public long getWindowWidthMillis() {
      return timeUnit.getDuration().multipliedBy(widthMultiple).toMillis();
    }

    @Override
    public long getWindowOffsetMillis() {
      return offsetUnit.getDuration().multipliedBy(offsetMultiple).toMillis();
    }
  }

//  @Override
//  public String getDocumentation() {
//    Instant DEFAULT_DOC_TIMESTAMP = Instant.parse("2023-03-12T18:23:34.083Z");
//    String functionCall = String.format("%s(%s(%s))", getFunctionName(),
//        STRING_TO_TIMESTAMP.getFunctionName(), DEFAULT_DOC_TIMESTAMP.toString());
//    String result = this.eval(DEFAULT_DOC_TIMESTAMP).toString();
//
//    String timeUnitName = timeUnit.toString().toLowerCase();
//    String timeUnitNameSingular = StringUtil.removeFromEnd(timeUnitName, "s");
//    String offsetUnitName = offsetUnit.toString().toLowerCase();
//    return String.format(
//        "Time window function that returns the end of %s for the timestamp argument."
//            + "<br />E.g. `%s` returns the timestamp `%s`."
//            + "<br />An optional second argument specifies the time window width as multiple %s, e.g. if the argument is 5 the function returns the end of the next 5 %s. 1 is the default."
//            + "<br />An optional third argument specifies the time window offset in %s, e.g. if the argument is 2 the function returns the end of the time window offset by 2 %s",
//        timeUnitNameSingular, functionCall, result, timeUnitName, timeUnitName, offsetUnitName,
//        offsetUnitName);
//  }

}
