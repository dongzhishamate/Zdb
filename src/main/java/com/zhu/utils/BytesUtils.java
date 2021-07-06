package com.zhu.utils;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BytesUtils {

  public final static void writeInt(OutputStream os, int i) throws IOException {
    os.write(i >> 24);
    os.write(i >> 16);
    os.write(i >> 8);
    os.write(i);
  }

  public static int decodeIntFromBytesArray(byte[] bytes, int offset) {
    return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) |
            ((bytes[offset + 2] & 0xFF) << 8) | ((bytes[offset + 3] & 0xFF));
  }

  public final static int readInt(InputStream is) throws IOException {
    return (is.read() & 0xFF) << 24 | (is.read() & 0xFF) << 16 | (is.read() & 0xFF) << 8
            | (is.read() & 0xFF);
  }

}
