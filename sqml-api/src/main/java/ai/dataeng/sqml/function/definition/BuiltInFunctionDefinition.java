//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package ai.dataeng.sqml.function.definition;

import ai.dataeng.sqml.function.UserDefinedFunction;
import ai.dataeng.sqml.function.definition.inference.InputTypeStrategy;
import ai.dataeng.sqml.function.definition.inference.TypeInference;
import ai.dataeng.sqml.function.definition.inference.TypeStrategy;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.NotImplementedException;

public final class BuiltInFunctionDefinition implements SpecializedFunction {
  private final String name;
  private final FunctionKind kind;
  private final TypeInference typeInference;
  private final boolean isDeterministic;

  private BuiltInFunctionDefinition(String name, FunctionKind kind, TypeInference typeInference, boolean isDeterministic) {
    this.name = Preconditions.checkNotNull(name, "Name must not be null.");
    this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
    this.typeInference = Preconditions.checkNotNull(typeInference, "Type inference must not be null.");
    this.isDeterministic = isDeterministic;
  }

  public static BuiltInFunctionDefinition.Builder newBuilder() {
    return new BuiltInFunctionDefinition.Builder();
  }

  public String getName() {
    return this.name;
  }

  public UserDefinedFunction specialize(SpecializedContext context) {
    throw new NotImplementedException("udf tbd");
  }

  public FunctionKind getKind() {
    return this.kind;
  }

  public TypeInference getTypeInference() {
    return this.typeInference;
  }

  public boolean isDeterministic() {
    return this.isDeterministic;
  }

  public String toString() {
    return this.name;
  }

  public static final class Builder {
    private String name;
    private FunctionKind kind;
    private final TypeInference.Builder typeInferenceBuilder = TypeInference.newBuilder();
    private boolean isDeterministic = true;

    public Builder() {
    }

    public BuiltInFunctionDefinition.Builder name(String name) {
      this.name = name;
      return this;
    }

    public BuiltInFunctionDefinition.Builder kind(FunctionKind kind) {
      this.kind = kind;
      return this;
    }

    public BuiltInFunctionDefinition.Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
      this.typeInferenceBuilder.inputTypeStrategy(inputTypeStrategy);
      return this;
    }

    public BuiltInFunctionDefinition.Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
      this.typeInferenceBuilder.outputTypeStrategy(outputTypeStrategy);
      return this;
    }

    public BuiltInFunctionDefinition.Builder notDeterministic() {
      this.isDeterministic = false;
      return this;
    }

    public BuiltInFunctionDefinition build() {
      return new BuiltInFunctionDefinition(this.name, this.kind, this.typeInferenceBuilder.build(), this.isDeterministic);
    }
  }
}
