package com.zhu.serde;

public class ZdbType {

  // only decide which character is placeholder when use like or not like
  private static final byte PLACEHOLDER = '_';

  public static final int UNKNOW = -1;
  public static final int BYTE = 0;
  public static final int SHORT = 1;
  public static final int BOOLEAN = 2;
  public static final int INT = 3;
  public static final int LONG = 4;
  public static final int FLOAT = 5;
  public static final int DOUBLE = 6;
  public static final int CHAR = 7;
  public static final int STRING = 8;
  public static final int VARCHAR2 = 9;
  public static final int ARRAY = 10;
  public static final int OTHER = 11;
  public static final int VOID = 12;
  public static final int WRDECIMAL = 13;
  public static final int VARCHAR = 14;
  public static final int TEXT = -2;
  public static final int TOTALTYPE = 15;

  public static boolean isNumericType(int t) {
    return isDigitalType(t) || isFloatType(t) || t == BOOLEAN;
  }

  public  static boolean isDigitalType(int t) {
    return t == BYTE || t == SHORT || t == INT || t == LONG;
  }

  public static boolean isStringType(int t) {
    return t == STRING || t == VARCHAR || t == CHAR || t == VARCHAR2;
  }

  // cube only support String and Varchar because char and varchar2 type need dialect info
  public static boolean isStringTypeCube(int t) {
    return t == STRING || t == VARCHAR;
  }

  public static boolean isFloatType(int t) {
    return t == FLOAT || t == DOUBLE;
  }

  public static boolean isTypeEqualByNatualConversion(int t1, int t2) {
    return t1 == t2 || (isDigitalType(t1) && isDigitalType(t2)) ||
            (isStringTypeCube(t1) && isStringTypeCube(t2)) || (isFloatType(t1) && isFloatType(t2));
  }

  public static byte getPlaceholder() {
    return PLACEHOLDER;
  }
}
