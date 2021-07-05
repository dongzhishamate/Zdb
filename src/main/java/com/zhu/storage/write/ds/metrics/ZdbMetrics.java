package com.zhu.storage.write.ds.metrics;

import java.io.Serializable;

public class ZdbMetrics implements Serializable {
  public long dataRead;
  public int readPieceNum;
  public int totalPieceNum;
  public int readRowBlockNum;
  public int totalRowBlockNum;
  public int skipHashNum;
  public int hashNum;

  public void addDataRead(long bytesRead) {
    dataRead += bytesRead;
  }

  public void addReadPieceNum(int readPiece) {
    this.readPieceNum += readPiece;
  }

  public void addTotalPieceNum(int totalPiece) {
    this.totalPieceNum += totalPiece;
  }

  public void addSkipHashNum(int skipHash) {
    skipHashNum += skipHash;
  }

  public void addHashNum(int hash) {
    hashNum += hash;
  }

}
