package com.datasqrl.loaders;

import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.NamespaceObject;
import java.net.URL;
import java.util.Optional;

public interface URLObjectLoader {

  Optional<NamespaceObject> load(URL url, ResourceResolver resourceResolver, NamePath namePath);
}
