package com.zhu.utils;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.serde.ZdbType;

public class ValueUtils {

  public static int compare(byte[] source, int offset1, int length1,
                            byte[] target, int offset2, int length2, int dataType) {
    int result;
    switch (dataType) {
      case ZdbType.BYTE: {
        result = Byte.compare(FastSerdeHelper.deByte(source, offset1),
                FastSerdeHelper.deByte(target, offset2));
        break;
      }
      case ZdbType.BOOLEAN: {
        result = Boolean.compare(FastSerdeHelper.deBoolean(source, offset1),
                FastSerdeHelper.deBoolean(target, offset2));
        break;
      }
      case ZdbType.SHORT: {
        result = Short.compare(FastSerdeHelper.deShort(source, offset1),
                FastSerdeHelper.deShort(target, offset2));
        break;
      }
      case ZdbType.INT: {
        result = Integer.compare(FastSerdeHelper.deInt(source, offset1),
                FastSerdeHelper.deInt(target, offset2));
        break;
      }
      case ZdbType.LONG: {
        result = Long.compare(FastSerdeHelper.deLong(source, offset1),
                FastSerdeHelper.deLong(target, offset2));
        break;
      }
      case ZdbType.FLOAT: {
        result = Float.compare(FastSerdeHelper.deFloat(source, offset1),
                FastSerdeHelper.deFloat(target, offset2));
        break;
      }
      case ZdbType.DOUBLE: {
        result = Double.compare(FastSerdeHelper.deDouble(source, offset1),
                FastSerdeHelper.deDouble(target, offset2));
        break;
      }
      case ZdbType.CHAR:
      case ZdbType.STRING:
      case ZdbType.VARCHAR:
      case ZdbType.VARCHAR2:
      case ZdbType.WRDECIMAL:
      case ZdbType.OTHER: {
        throw new RuntimeException("Unsupported holodesk type");
      }
      default: {
        throw new RuntimeException("Unsupported holodesk type");
      }
    }
    return result;
  }
}
