package ai.dataeng.sqml.rewrite;

import ai.dataeng.sqml.tree.Script;

public abstract class ScriptRewrite {
  public abstract Script rewrite(Script script);
}
