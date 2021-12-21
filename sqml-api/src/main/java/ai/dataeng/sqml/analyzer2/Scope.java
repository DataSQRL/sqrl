package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.NamePath;
import lombok.Value;

@Value
public class Scope {
  NamePath currentContext;
  SqrlEntity entity;
}
