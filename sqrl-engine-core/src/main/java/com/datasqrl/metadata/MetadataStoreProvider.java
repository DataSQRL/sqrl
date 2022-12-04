/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.metadata;

import java.io.Serializable;

public interface MetadataStoreProvider extends Serializable {

  MetadataStore openStore();

}
