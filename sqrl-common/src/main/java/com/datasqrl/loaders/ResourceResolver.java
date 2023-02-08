package com.datasqrl.loaders;

import com.datasqrl.name.NamePath;
import java.net.URL;
import java.util.List;

public interface ResourceResolver {

  List<URL> loadPath(NamePath namePath);

}
