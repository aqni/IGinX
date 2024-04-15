package cn.edu.tsinghua.iginx.parquet.io.parquet.exception;

import cn.edu.tsinghua.iginx.parquet.io.exception.FileException;

public class ParquetException extends FileException {
  public ParquetException(String message) {
    super(message);
  }

  public ParquetException(Throwable cause) {
    super(cause);
  }

  public ParquetException(String message, Throwable cause) {
    super(message, cause);
  }
}
