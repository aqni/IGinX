package cn.edu.tsinghua.iginx.parquet.io.exception;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class FileException extends PhysicalException {

  public FileException(String message) {
    super(message);
  }

  public FileException(Throwable cause) {
    super(cause);
  }

  public FileException(String message, Throwable cause) {
    super(message, cause);
  }
}
