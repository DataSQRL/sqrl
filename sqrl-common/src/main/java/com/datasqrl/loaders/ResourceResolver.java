package com.datasqrl.loaders;

import com.datasqrl.name.NamePath;

import java.net.URI;
import java.util.List;

public interface ResourceResolver {

  List<URI> loadPath(NamePath namePath);

}
