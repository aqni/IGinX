package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.parquet.io.exception.FileException;
import com.google.common.collect.Range;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Optional;

@NotThreadSafe
public abstract class FileRowStream implements RowStream {

  @Override
  public abstract Header getHeader() throws FileException;

  public abstract Optional<Range<Long>> getRange() throws FileException;

  @Override
  public abstract void close() throws FileException;

  @Override
  public abstract boolean hasNext() throws FileException;

  @Override
  public abstract Row next() throws FileException;

  @Override
  @Deprecated
  public final void setContext(RequestContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public final RequestContext getContext() {
    throw new UnsupportedOperationException();
  }
}
