package com.zhu.storage.write.ds;

import com.zhu.result.ByteArrayColumnResult;
import com.zhu.result.ColumnResult;
import com.zhu.storage.write.RandomAccessFileOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import org.junit.Test;

import javax.xml.soap.Text;
import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.Assert.*;

public class HashDictionaryColumnBlockDSTest {

  @Test
  public void testSpill() throws IOException {
    HashDictionaryColumnBlockDS ds = new HashDictionaryColumnBlockDS();
    ds.initBuilder();
    byte[] testData = "12223".getBytes();
    byte[] testData1 = "12224".getBytes();
    ds.put(testData, 0, testData.length);
    ds.put(testData1, 0, testData1.length);
    RandomAccessFile raf = new RandomAccessFile("text.txt", "rw");
    RandomAccessFileOutputStream raOut = new RandomAccessFileOutputStream();
    raOut.setRandomAccessFile(raf);
    ds.spill(raOut);
    raOut.flush();
  }

  @Test
  public void testDecode() throws IOException {
    HashDictionaryColumnBlockDS ds = new HashDictionaryColumnBlockDS();
    File file = new File("text.txt");
    long length = file.length();
    MappedByteBuffer buffer = new RandomAccessFile(file, "r")
            .getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);
    ds.load(buffer, 0);
    ds.decode(true);
  }

  @Test
  public void testDirectGet() throws IOException {
    HashDictionaryColumnBlockDS ds = new HashDictionaryColumnBlockDS();
    File file = new File("text.txt");
    long length = file.length();
    MappedByteBuffer buffer = new RandomAccessFile(file, "r")
            .getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);
    ds.load(buffer, 0);
    ByteArrayColumnResult columnResult = (ByteArrayColumnResult) ds.directGet(1);
    System.out.println(ds.getRowNum());
    System.out.println(ds.getDicSize());
    System.out.println(columnResult.getLength());
    System.out.println(columnResult.getData().length);
    System.out.println(new String(columnResult.getData()));
  }

  @Test
  public void test() throws IOException {
    byte[] bytes = "12345".getBytes();
    File file = new File("test2.txt");
    FileOutputStream out = new FileOutputStream(file);
    out.write(bytes);
    System.out.println(new String(bytes));
  }
}
