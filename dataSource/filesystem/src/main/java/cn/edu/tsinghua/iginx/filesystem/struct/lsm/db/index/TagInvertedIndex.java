package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.index;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class TagInvertedIndex {
  private final Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, LongArrayList>> index =
      new Object2ObjectOpenHashMap<>();

  

}
