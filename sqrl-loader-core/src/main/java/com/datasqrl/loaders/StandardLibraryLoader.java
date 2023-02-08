package com.datasqrl.loaders;

import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.NamespaceObject;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StandardLibraryLoader {
  private final Map<NamePath, SqrlModule> standardLibrary;

  public List<NamespaceObject> load(NamePath namePath) {
    if (standardLibrary.containsKey(namePath)) {
      return standardLibrary.get(namePath).getNamespaceObjects();
    }
    return List.of();
  }
}