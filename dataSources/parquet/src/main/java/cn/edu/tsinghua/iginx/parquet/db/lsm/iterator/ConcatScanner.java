/*
 * Copyright 2024 IGinX of Tsinghua University
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

package cn.edu.tsinghua.iginx.parquet.db.lsm.iterator;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class ConcatScanner<K extends Comparable<K>, V> implements Scanner<K, V> {

  private final Iterator<Scanner<K, V>> scannerIterator;

  private Scanner<K, V> currentScanner;

  public ConcatScanner(Iterator<Scanner<K, V>> iterator) {
    this.scannerIterator = iterator;
  }

  @Nonnull
  @Override
  public K key() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    return currentScanner.key();
  }

  @Nonnull
  @Override
  public V value() throws NoSuchElementException {
    if (currentScanner == null) {
      throw new NoSuchElementException();
    }
    return currentScanner.value();
  }

  @Override
  public boolean iterate() throws IOException {
    if (currentScanner != null && currentScanner.iterate()) {
      return true;
    }
    while (scannerIterator.hasNext()) {
      currentScanner = scannerIterator.next();
      if (currentScanner.iterate()) {
        return true;
      }
    }
    currentScanner = null;
    return false;
  }

  @Override
  public void close() throws IOException {
    while (scannerIterator.hasNext()) {
      scannerIterator.next().close();
    }
  }
}
