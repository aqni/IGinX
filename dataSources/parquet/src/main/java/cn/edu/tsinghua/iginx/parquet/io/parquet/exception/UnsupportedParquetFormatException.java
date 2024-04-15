package cn.edu.tsinghua.iginx.parquet.io.parquet.exception;

import cn.edu.tsinghua.iginx.parquet.io.exception.UnsupportedFileFormatException;

public class UnsupportedParquetFormatException extends UnsupportedFileFormatException {

  public UnsupportedParquetFormatException(String message) {
    super(message);
  }
    
}
