package com.zhu.storage.write;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class RandomAccessFileOutputStream extends OutputStream {

  private RandomAccessFile raf = null;
  private final int BufferSize = 4 * 1024;
  private int randomSeed = (int)(1024 * Math.random());
  private FastByteArrayOutputStream writeBuffer = new FastByteArrayOutputStream(BufferSize);
  private  int flushSeed = 0;

  private final boolean needFlush() {
    return writeBuffer.length > flushSeed;
  }

  public RandomAccessFileOutputStream() {
    flushSeed = BufferSize - randomSeed;
  }

  public void setRandomAccessFile(RandomAccessFile raf) {
    this.raf = raf;
  }

  public RandomAccessFileOutputStream(RandomAccessFile raf) {
    this.raf = raf;
    flushSeed = BufferSize - randomSeed;
  }

  @Override
  public void write(int b) throws IOException {
    writeBuffer.write(b);
    if (needFlush()) {
      flush();
    }
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    writeBuffer.write(b, off, len);
    if (needFlush()) {
      flush();
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    if (raf != null) {
      raf.close();
    }
  }

  @Override
  public void flush() throws IOException {
    if (raf != null && writeBuffer.length > 0) {
      raf.write(writeBuffer.array, 0, writeBuffer.length);
      writeBuffer.reset();
    }
  }
}
