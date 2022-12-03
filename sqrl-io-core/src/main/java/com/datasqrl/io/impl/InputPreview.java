package com.datasqrl.io.impl;

import java.io.BufferedReader;
import java.util.stream.Stream;

public interface InputPreview {

  Stream<BufferedReader> getTextPreview();

}
