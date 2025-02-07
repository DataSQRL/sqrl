package com.datasqrl.v2.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.datasqrl.v2.parser.StatementParserException;
import com.datasqrl.plan.rules.SqrlConverterConfig.SqrlConverterConfigBuilder;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.ServiceLoaderException;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Getter;

@Getter
public abstract class PlannerHint implements Hint {

  protected final ParsedObject<SqrlHint> source;
  protected final Type type;


  protected PlannerHint(ParsedObject<SqrlHint> source, Type type) {
    Preconditions.checkArgument(source.isPresent());
    this.source = source;
    this.type = type;
  }

  public List<String> getOptions() {
    return source.get().getOptions();
  }

  public String getName() {
    return source.get().getName().toLowerCase();
  }


  public enum Type {
    ANALYZER, DAG
  }

  public static PlannerHint from(ParsedObject<SqrlHint> sqrlHint) {
    Preconditions.checkArgument(sqrlHint.isPresent());
    try {
      Factory factory = ServiceLoaderDiscovery.get(Factory.class, Factory::getName,
          sqrlHint.get().getName());
      return factory.create(sqrlHint);
    } catch (ServiceLoaderException e) {
      throw new StatementParserException(ErrorLabel.GENERIC, sqrlHint.getFileLocation(), "Unrecognized hint [%s]", sqrlHint.get().getName());
    }
  }

  public interface Factory {
    PlannerHint create(ParsedObject<SqrlHint> source);
    String getName();
  }

}
