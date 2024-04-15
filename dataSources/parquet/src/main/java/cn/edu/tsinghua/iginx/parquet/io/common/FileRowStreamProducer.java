package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.parquet.io.exception.FileException;

import java.net.URI;

public interface FileRowStreamProducer {
      FileRowStream produce(URI uri) throws FileException;
}
