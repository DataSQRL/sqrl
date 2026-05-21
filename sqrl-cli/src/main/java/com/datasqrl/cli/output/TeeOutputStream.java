/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.cli.output;

import java.io.IOException;
import java.io.OutputStream;

class TeeOutputStream extends OutputStream {

  private final OutputStream left;
  private final OutputStream right;

  TeeOutputStream(OutputStream left, OutputStream right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public void write(int b) throws IOException {
    left.write(b);
    right.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    left.write(b, off, len);
    right.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    left.flush();
    right.flush();
  }

  @Override
  public void close() throws IOException {
    flush();
  }
}
