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

package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Prefetch;
import java.io.IOException;

public class NoPrefetch implements Prefetch {

  @Override
  public Prefetcher prefetch(Prefetchable prefetchable) {
    return new Prefetcher() {
      @Override
      public Object get(long id) throws IOException {
        return prefetchable.fetch(id);
      }

      @Override
      public void close() {}
    };
  }
}
