package com.zhu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() {
    if (buffer == null || buffer.remaining() == 0) {
      return -1;
    } else {
      return buffer.get() & 0xFF;
    }
  }

  @Override
  public int read(byte[] dest) {
    return read(dest, 0, dest.length);
  }

  @Override
  public int read(byte[] dest, int offset, int length) {
    if (buffer == null || buffer.remaining() == 0) {
      return -1;
    } else {
      int amountToGet = Math.min(buffer.remaining(), length);
      buffer.get(dest, offset, amountToGet);
      return amountToGet;
    }
  }

  @Override
  public long skip(long bytes) {
    if (buffer != null) {
      int amountToGet = (int)Math.min(buffer.remaining(), bytes);
      buffer.position(buffer.position() + amountToGet);
      return amountToGet;
    } else {
      return 0L;
    }
  }
}
