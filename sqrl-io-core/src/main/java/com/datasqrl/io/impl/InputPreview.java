/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl;

import java.io.BufferedReader;
import java.util.stream.Stream;

public interface InputPreview {

  Stream<BufferedReader> getTextPreview();

}
