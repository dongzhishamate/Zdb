package com.zhu.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtils {
  private static long charArrayOffset;
  private static long byteArrayOffset;
  private static long booleanArrayOffset;
  private static long shortArrayOffset;
  private static long intArrayOffset;
  private static long longArrayOffset;
  private static long doubleArrayOffset;
  private static long floatArrayOffset;
  private static Unsafe unsafe;

  static {
    {
      try {
        Field f = null;
        Field[] fields = Unsafe.class.getDeclaredFields();
        for (Field field : fields) {
          if (field.getName().contains("theUnsafe")) {
            f = field;
            break;
          }
        }
        if (f == null) {
          throw new RuntimeException("JDK version doesn't support unsafe method.");
        }
        f.setAccessible(true);
        try {
          unsafe = (Unsafe) f.get(null);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        charArrayOffset = unsafe.arrayBaseOffset(char[].class);
        byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);
        booleanArrayOffset = unsafe.arrayBaseOffset(boolean[].class);
        shortArrayOffset = unsafe.arrayBaseOffset(short[].class);
        intArrayOffset = unsafe.arrayBaseOffset(int[].class);
        longArrayOffset = unsafe.arrayBaseOffset(long[].class);
        doubleArrayOffset = unsafe.arrayBaseOffset(double[].class);
        floatArrayOffset = unsafe.arrayBaseOffset(float[].class);
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (SecurityException e) {
        e.printStackTrace();
      }
    }
  }

  public static long getCharArrayOffset() {
    return charArrayOffset;
  }

  public static long getByteArrayOffset() {
    return byteArrayOffset;
  }

  public static long getBooleanArrayOffset() {
    return booleanArrayOffset;
  }

  public static long getShortArrayOffset() {
    return shortArrayOffset;
  }

  public static long getIntArrayOffset() {
    return intArrayOffset;
  }

  public static long getLongArrayOffset() {
    return longArrayOffset;
  }

  public static long getDoubleArrayOffset() {
    return doubleArrayOffset;
  }

  public static long getFloatArrayOffset() {
    return floatArrayOffset;
  }
  public static Unsafe getUnsafe() {
    return unsafe;
  }
}
