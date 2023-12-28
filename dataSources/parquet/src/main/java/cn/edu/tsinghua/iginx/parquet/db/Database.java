package cn.edu.tsinghua.iginx.parquet.db;

import cn.edu.tsinghua.iginx.parquet.entity.NativeStorageException;
import cn.edu.tsinghua.iginx.parquet.entity.Range;
import cn.edu.tsinghua.iginx.parquet.entity.RangeSet;
import cn.edu.tsinghua.iginx.parquet.entity.Scanner;
import java.util.Set;

public interface Database<K extends Comparable<K>, F, V> extends AutoCloseable {
  Scanner<K, Scanner<F, V>> query(Set<F> fields, Range<K> range) throws NativeStorageException;

  void upsertRows(Scanner<K, Scanner<F, V>> scanner) throws NativeStorageException;

  void upsertColumns(Scanner<F, Scanner<K, V>> scanner) throws NativeStorageException;

  void delete(Set<F> fields, RangeSet<K> ranges) throws NativeStorageException;

  void deleteRows(Set<F> fields) throws NativeStorageException;

  void deleteColumns(RangeSet<K> ranges) throws NativeStorageException;

  void clear() throws NativeStorageException;

  @Override
  void close();
}
