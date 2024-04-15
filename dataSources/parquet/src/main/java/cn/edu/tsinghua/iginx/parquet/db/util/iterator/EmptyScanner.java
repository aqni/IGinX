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

<<<<<<<< HEAD:dataSources/parquet/src/main/java/cn/edu/tsinghua/iginx/parquet/db/util/iterator/EmptyScanner.java
package cn.edu.tsinghua.iginx.parquet.db.util.iterator;
========
package cn.edu.tsinghua.iginx.parquet.util.iterator;
>>>>>>>> refs/heads/feature-improve_dummy_preformance_of_parquet:dataSources/parquet/src/main/java/cn/edu/tsinghua/iginx/parquet/util/iterator/EmptyScanner.java

import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;

public class EmptyScanner<K, V> implements Scanner<K, V> {

  private static final Scanner<?, ?> EMPTY = new EmptyScanner<>();

  @SuppressWarnings("unchecked")
  public static <K, V> Scanner<K, V> getInstance() {
    return (Scanner<K, V>) EMPTY;
  }

  @Nonnull
  @Override
  public K key() {
    throw new NoSuchElementException();
  }

  @Nonnull
  @Override
  public V value() {
    throw new NoSuchElementException();
  }

  @Override
  public boolean iterate() throws StorageException {
    return false;
  }

  @Override
  public void close() throws StorageException {}
}
