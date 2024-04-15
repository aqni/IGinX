package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.parquet.io.exception.FileException;

import java.io.Closeable;
import java.net.URI;

public interface FileRowStreamConsumer extends Closeable {
  void consume(URI uri, RowStream rowStream) throws FileException;
}
