package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

public class TypeUtils {


  private TypeUtils() {
  }
  
  public static IMeasurementSchema toTsfileField(Field field) {
    TSDataType type = toTsfileType(field.getType());
    return new MeasurementSchema(
        field.getName(),
        toTsfileType(field.getType()),
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder(type)),
        TSFileDescriptor.getInstance().getConfig().getCompressor(type),
        field.getTags());
  }

  public static TSDataType toTsfileType(DataType type) {
    switch (type) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case INTEGER:
        return TSDataType.INT32;
      case LONG:
        return TSDataType.INT64;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case BINARY:
        return TSDataType.BLOB;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

}
