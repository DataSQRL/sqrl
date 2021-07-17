package ai.dataeng.sqml.registry;

import ai.dataeng.sqml.function.FunctionHandle;
import ai.dataeng.sqml.model.Model;

public abstract class ScriptRegistry {
  public abstract Model resolveScript();
  public abstract FunctionHandle resolveFunction();
}
