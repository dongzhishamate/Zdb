package com.zhu.storage.write;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.*;

public class RandomAccessFileOutputStreamTest {

  @Test
  public void testRandomAccessFileOutputStream() {
    RandomAccessFile raf = null;
    RandomAccessFileOutputStream raOut = null;
    try {
      raf = new RandomAccessFile("test1.txt", "rw");
      raOut = new RandomAccessFileOutputStream(raf);
      raOut.write("12345 aaa".getBytes());
      System.out.println("12345 aaa".getBytes());
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        raOut.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
