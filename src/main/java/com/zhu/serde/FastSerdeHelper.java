package com.zhu.serde;

import com.zhu.result.ByteArrayColumnResult;
import com.zhu.result.ColumnResult;
import com.zhu.utils.UnsafeUtils;
import sun.nio.ch.DirectBuffer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class FastSerdeHelper {
  public final static int BooleanSize = 1;
  public final static int ByteSize = 1;
  public final static int ShortSize = 2;
  public final static int CharSize = 2;
  public final static int IntSize = 4;
  public final static int LongSize = 8;
  public final static int FloatSize = 4;
  public final static int DoubleSize = 8;
  public final static int OtherSize = -1;

  public final static int BooleanShiftSize = 0;
  public final static int ByteShiftSize = 0;
  public final static int CharShiftSize = 1;
  public final static int ShortShiftSize = 1;
  public final static int IntShiftSize = 2;
  public final static int LongShiftSize = 3;
  public final static int FloatShiftSize = 2;
  public final static int DoubleShiftSize = 3;


  public static byte[] serialize(Object v, int type) {
    byte[] res;
    if (v == null) {
      res = null;
    } else {
      switch (type) {
          case ZdbType.BYTE:
          res = serByte((Byte) v);
          break;
        case ZdbType.BOOLEAN:
          res = serBoolean((Boolean) v);
          break;
        case ZdbType.SHORT:
          res = serShort((Short) v);
          break;
        case ZdbType.INT:
          res = serInt((Integer) v);
          break;
        case ZdbType.LONG:
          res = serLong((Long) v);
          break;
        case ZdbType.FLOAT:
          res = serFloat((Float) v);
          break;
        case ZdbType.DOUBLE:
          res = serDouble((Double) v);
          break;
        case ZdbType.CHAR:
        case ZdbType.STRING:
        case ZdbType.VARCHAR:
        case ZdbType.VARCHAR2:
          throw new RuntimeException("[ZDB] Unsupported type");
//          res = serText((Text) v);
        case ZdbType.OTHER:
          res = serOther((byte[]) v);
          break;
        default:
          throw new RuntimeException("[ZDB] Unsupported type");
      }
    }
    return res;
  }

//  public static boolean isNull (ColumnResult columnResult) {
//  }
//
//  public static Object deserialize(ColumnResult columnResult, int dataType) {
//    if (isNull(columnResult)) {
//      return null;
//    } else {
//      return deserialize(columnResult.getRef(), columnResult.getStart(), columnResult.getLength(), dataType);
//    }
//  }

  public static Object deserialize(byte[] data, int dataType) {
    if (data == null) {
      return null;
    } else {
      return deserialize(data, 0, data.length, dataType);
    }
  }

  public static Object deserialize(byte[] data, int offset, int length, int dataType) {
    Object result;
    switch (dataType) {
      case ZdbType.BYTE:
        result = deByte(data, offset);
        break;
      case ZdbType.BOOLEAN:
        result = deBoolean(data, offset);
        break;
      case ZdbType.SHORT:
        result = deShort(data, offset);
        break;
      case ZdbType.INT:
        result = deInt(data, offset);
        break;
      case ZdbType.LONG:
        result = deLong(data, offset);
        break;
      case ZdbType.FLOAT:
        result = deFloat(data, offset);
        break;
      case ZdbType.DOUBLE:
        result = deDouble(data, offset);
        break;
      case ZdbType.CHAR:
      case ZdbType.STRING:
      case ZdbType.VARCHAR:
      case ZdbType.VARCHAR2:
        throw new RuntimeException("[ZDB] Unsupported type");
      case ZdbType.OTHER:
        result = deOtherBatch(ByteBuffer.wrap(data), offset, length);
        break;
      case ZdbType.VOID:
      case ZdbType.WRDECIMAL:
      case ZdbType.ARRAY:

      default:
        throw new RuntimeException("[ZDB] Unsupported type");
    }
    return result;
  }

  public static byte[] serByte(byte v) {
    byte[] b = new byte[ByteSize];
    UnsafeUtils.getUnsafe().putByte(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static byte[] serBoolean(boolean v) {
    byte[] b = new byte[BooleanSize];
    UnsafeUtils.getUnsafe().putBoolean(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static byte[] serShort(short v) {
    byte[] b = new byte[ShortSize];
    UnsafeUtils.getUnsafe().putShort(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static void serShort(short v, byte[] res, int offset) {
    UnsafeUtils.getUnsafe().putShort(res, UnsafeUtils.getByteArrayOffset() + offset, v);
  }

  public static void serInt(int v, byte[] res, int offset) {
    UnsafeUtils.getUnsafe().putInt(res, UnsafeUtils.getByteArrayOffset() + offset, v);
  }

  public static byte[] serInt(int v) {
    byte[] b = new byte[IntSize];
    UnsafeUtils.getUnsafe().putInt(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static byte[] serLong(long v) {
    byte[] b = new byte[LongSize];
    UnsafeUtils.getUnsafe().putLong(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static void serLong(long v, byte[] res, int offset) {
    UnsafeUtils.getUnsafe().putLong(res, UnsafeUtils.getByteArrayOffset() + offset, v);
  }

  public static byte[] serFloat(float v) {
    byte[] b = new byte[FloatSize];
    UnsafeUtils.getUnsafe().putFloat(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static byte[] serDouble(double v) {
    byte[] b = new byte[DoubleSize];
    UnsafeUtils.getUnsafe().putDouble(b, UnsafeUtils.getByteArrayOffset(), v);
    return b;
  }

  public static void serDouble(double v, byte[] res, int offset) {
    UnsafeUtils.getUnsafe().putDouble(res, UnsafeUtils.getByteArrayOffset() + offset, v);
  }

  // only used in crud
  public static byte[] serString(String v) {
    return v.getBytes();
  }


  public static byte[] serOther(byte[] v) {
    byte[] res = new byte[v.length];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getByteArrayOffset(),
            res, UnsafeUtils.getByteArrayOffset(), v.length);
    return res;
  }

  public static byte[] serByteBatch(byte[] v, int size) {
    byte[] b = new byte[size];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getByteArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size);
    return b;
  }

  public static byte[] serBooleanBatch(boolean[] v, int size) {
    byte[] b = new byte[size];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getBooleanArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size);
    return b;
  }

  public static byte[] serShortBatch(short[] v, int size) {
    byte[] b = new byte[size << ShortShiftSize];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getShortArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size << ShortShiftSize);
    return b;
  }

  public static byte[] serIntBatch(int[] v, int size) {
    byte[] b = new byte[size << IntShiftSize];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getIntArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size << IntShiftSize);
    return b;
  }

  public static byte[] serLongBatch(long[] v, int size) {
    byte[] b = new byte[size << LongShiftSize];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getLongArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size << LongShiftSize);
    return b;
  }

  public static byte[] serFloatBatch(float[] v, int size) {
    byte[] b = new byte[size << FloatShiftSize];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getFloatArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size << FloatShiftSize);
    return b;
  }

  public static byte[] serDoubleBatch(double[] v, int size) {
    byte[] b = new byte[size << DoubleShiftSize];
    UnsafeUtils.getUnsafe().copyMemory(v, UnsafeUtils.getDoubleArrayOffset(),
            b, UnsafeUtils.getByteArrayOffset(), size << DoubleShiftSize);
    return b;
  }

  public static byte[] deByteBatch(ByteBuffer buffer, int offset, int size) {
    byte[] result = new byte[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getByteArrayOffset(), size);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getByteArrayOffset(), size);
    }
    return result;
  }

  public static boolean[] deBooleanBatch(ByteBuffer buffer, int offset, int size) {
    boolean[] result = new boolean[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getBooleanArrayOffset(), size);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getBooleanArrayOffset(), size);
    }
    return result;
  }

  public static char[] deCharBatch(ByteBuffer buffer, int offset, int length) {
    // 1 char = 2 bytes
    char[] result = new char[length >> CharShiftSize];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getCharArrayOffset(), length);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getCharArrayOffset(), length);
    }
    return result;
  }

  public static char[][] deFixedLengthCharBatch(ByteBuffer buffer, int offset, int length, int fixedLength) {
    char[][] result = new char[(length / fixedLength)][fixedLength >> CharShiftSize];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      for (int i = 0; i < result.length; i++) {
        UnsafeUtils.getUnsafe().copyMemory(null, address + fixedLength * i, result[i], UnsafeUtils.getCharArrayOffset(), fixedLength);
      }
    } else {
      for (int i = 0; i < result.length; i++) {
        UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset + fixedLength * i,
                result[i], UnsafeUtils.getCharArrayOffset(), fixedLength);
      }
    }
    return result;
  }

  public static short[] deShortBatch(ByteBuffer buffer, int offset, int size) {
    short[] result = new short[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getShortArrayOffset(), size << ShortShiftSize);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getShortArrayOffset(), size << ShortShiftSize);
    }

    return result;
  }

  public static int[] deIntBatch(ByteBuffer buffer, int offset, int size) {
    int[] result = new int[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getIntArrayOffset(), size << IntShiftSize);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getIntArrayOffset(), size << IntShiftSize);
    }
    return result;
  }

  public static float[] deFloatBatch(ByteBuffer buffer, int offset, int size) {
    float[] result = new float[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getFloatArrayOffset(), size << FloatShiftSize);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getFloatArrayOffset(), size << FloatShiftSize);
    }
    return result;
  }

  public static long[] deLongBatch(ByteBuffer buffer, int offset, int size) {
    long[] result = new long[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getLongArrayOffset(), size << LongShiftSize);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getLongArrayOffset(), size << LongShiftSize);
    }
    return result;
  }

  public static double[] deDoubleBatch(ByteBuffer buffer, int offset, int size) {
    double[] result = new double[size];
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      UnsafeUtils.getUnsafe().copyMemory(null, address, result, UnsafeUtils.getDoubleArrayOffset(), size << DoubleShiftSize);
    } else {
      UnsafeUtils.getUnsafe().copyMemory(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset,
              result, UnsafeUtils.getDoubleArrayOffset(), size << DoubleShiftSize);
    }
    return result;
  }

  public static byte[] deTextBatch(ByteBuffer buffer, int offset, int length) {
    return deByteBatch(buffer, offset, length);
  }

  public static byte[] deOtherBatch(ByteBuffer buffer, int offset, int length) {
    return deByteBatch(buffer, offset, length);
  }


  public static byte deByte(ByteBuffer buffer, int offset) {
    byte result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getByte(address);
    } else {
      result = UnsafeUtils.getUnsafe().getByte(buffer.array(), UnsafeUtils.getByteArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static boolean deBoolean(ByteBuffer buffer, int offset) {
    boolean result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getBoolean(null, address);
    } else {
      result = UnsafeUtils.getUnsafe().getBoolean(buffer.array(), UnsafeUtils.getBooleanArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static short deShort(ByteBuffer buffer, int offset) {
    short result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getShort(address);
    } else {
      result = UnsafeUtils.getUnsafe().getShort(buffer.array(), UnsafeUtils.getShortArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static int deInt(ByteBuffer buffer, int offset) {
    int result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getInt(address);
    } else {
      result = UnsafeUtils.getUnsafe().getInt(buffer.array(), UnsafeUtils.getIntArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static long deLong(ByteBuffer buffer, int offset) {
    long result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getLong(address);
    } else {
      result = UnsafeUtils.getUnsafe().getLong(buffer.array(), UnsafeUtils.getLongArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static float deFloat(ByteBuffer buffer, int offset) {
    float result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getFloat(address);
    } else {
      result = UnsafeUtils.getUnsafe().getFloat(buffer.array(), UnsafeUtils.getFloatArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static double deDouble(ByteBuffer buffer, int offset) {
    double result;
    if (!buffer.hasArray()) {
      long address = ((DirectBuffer)buffer).address() + offset;
      result = UnsafeUtils.getUnsafe().getDouble(address);
    } else {
      result = UnsafeUtils.getUnsafe().getDouble(buffer.array(), UnsafeUtils.getDoubleArrayOffset() + buffer.arrayOffset() + offset);
    }
    return result;
  }

  public static byte deByte(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getByte(data, UnsafeUtils.getByteArrayOffset() + offset);
  }

  public static boolean deBoolean(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getBoolean(data, UnsafeUtils.getBooleanArrayOffset() + offset);
  }

  public static short deShort(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getShort(data, UnsafeUtils.getShortArrayOffset() + offset);
  }

  public static int deInt(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getInt(data, UnsafeUtils.getIntArrayOffset() + offset);
  }

  public static long deLong(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getLong(data, UnsafeUtils.getLongArrayOffset() + offset);
  }

  public static float deFloat(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getFloat(data, UnsafeUtils.getFloatArrayOffset() + offset);
  }

  public static double deDouble(byte[] data, int offset) {
    return UnsafeUtils.getUnsafe().getDouble(data, UnsafeUtils.getDoubleArrayOffset() + offset);
  }

  public static char[] deChar(byte[] data, int offset, int length) {
    // 1 char = 2 bytes
    char[] result = new char[length >> CharShiftSize];
    UnsafeUtils.getUnsafe().copyMemory(data, UnsafeUtils.getByteArrayOffset() + offset,
            result, UnsafeUtils.getCharArrayOffset(), length);
    return result;
  }

  public static String deString(byte[] data, int offset, int length) {
    return new String(data, offset, length);
  }


  public static byte[] deOther(byte[] data, int offset, int length) {
    byte[] res = new byte[length];
    System.arraycopy(data, offset, res, 0, length);
    return res;
  }

  public static byte[] getNullValue(int type, int length) {
    byte[] nullV;
    switch(type) {
      case (ZdbType.BYTE): {
        nullV = new byte[ByteSize];
        break;
      }
      case (ZdbType.BOOLEAN): {
        nullV = new byte[BooleanSize];
        break;
      }
      case (ZdbType.SHORT): {
        nullV = new byte[ShortSize];
        break;
      }
      case (ZdbType.INT): {
        nullV = new byte[IntSize];
        break;
      }
      case (ZdbType.LONG): {
        nullV = new byte[LongSize];
        break;
      }
      case (ZdbType.FLOAT): {
        nullV = new byte[FloatSize];
        break;
      }
      case (ZdbType.DOUBLE): {
        nullV = new byte[DoubleSize];
        break;
      }
      case (ZdbType.CHAR): {
        nullV = new byte[length];
        break;
      }
      case (ZdbType.WRDECIMAL): {
        nullV = new byte[IntSize];
        break;
      }
      default: {
        nullV = new byte[1];
      }
    }
    return nullV;
  }


  public static void main(String[] args) throws UnsupportedEncodingException {
    String str = "全国量收系统";
    byte[] a = str.getBytes("utf-8");
    byte[] b = FastSerdeHelper.serString(str);

    ByteBuffer buffer = ByteBuffer.allocateDirect(b.length);
    buffer.put(b);
    long address = ((DirectBuffer)buffer).address();
    char[] c = new char[6];
    UnsafeUtils.getUnsafe().copyMemory(null, address, c, UnsafeUtils.getByteArrayOffset(), b.length);
    System.out.println(c[0]);
    System.out.println(c[1]);
    System.out.println(c[2]);
  }
}
