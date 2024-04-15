package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.io.exception.FileException;

import java.util.List;

public interface FileRowStreamAdapter {
  FileRowStreamProducer read(List<String> columns, Filter filter) throws FileException;
}
