package com.zhu.storage.write.ds;

import org.junit.Test;

import static org.junit.Assert.*;

public class RowBlockDSTest {

  @Test
  public void getKey() {
    RowBlockDS rowBlockDS = new RowBlockDS();
    int key = rowBlockDS.getKey(1, 1);
    System.out.println(key);
  }
}
