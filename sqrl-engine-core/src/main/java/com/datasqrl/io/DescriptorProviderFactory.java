/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import java.util.ServiceLoader;

public class DescriptorProviderFactory {

  public void load() {
    ServiceLoader<DescriptorProvider> serviceLoader = ServiceLoader.load(DescriptorProvider.class);
    for (DescriptorProvider provider : serviceLoader) {
      System.out.println(provider.getClass());
    }
  }
}
