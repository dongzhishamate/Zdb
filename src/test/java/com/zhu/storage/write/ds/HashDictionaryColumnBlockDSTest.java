package com.zhu.storage.write.ds;

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
    byte[] testData = "123".getBytes();
    ds.put(testData, 0, testData.length);
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
}
