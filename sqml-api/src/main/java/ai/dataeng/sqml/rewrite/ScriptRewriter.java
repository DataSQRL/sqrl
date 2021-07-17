package ai.dataeng.sqml.rewrite;

import ai.dataeng.sqml.tree.Script;
import java.util.List;

public class ScriptRewriter {

  private final List<ScriptRewrite> rewrites;

  public ScriptRewriter(List<ScriptRewrite> rewrites) {
    this.rewrites = rewrites;
  }

  public Script rewrite(Script script) {
    Script rewritten = script;
    for (ScriptRewrite rewrite : rewrites) {
      rewritten = rewrite.rewrite(rewritten);
    }
    return rewritten;
  }
}
