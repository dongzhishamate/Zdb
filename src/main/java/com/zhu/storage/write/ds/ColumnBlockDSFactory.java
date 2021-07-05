package com.zhu.storage.write.ds;

import com.zhu.storage.write.ds.ColumnBlockDS;
import com.zhu.storage.write.ds.HashDictionaryColumnBlockDS;
import com.zhu.storage.write.ds.RadixTrieColumnBlockDS;
import com.zhu.storage.write.ds.SortedHashDictionaryColumnBlockDS;

public class ColumnBlockDSFactory {

  public enum ColumnBlockType {
    RadixTrie,
    HashDict,
    SortedHash
  }

  private static ColumnBlockType int2type(int idx) {
    ColumnBlockType[] types = ColumnBlockType.values();
    if (idx > 0xFF || idx >= types.length || idx < 0) {
      throw new RuntimeException("Column Block Type Id not correct");
    }
    return types[idx];
  }

  public static ColumnBlockDS createCBDInstance(int blockType, int colType, boolean needMarkIsModifiedByCrud) {
    ColumnBlockType cbt = int2type(blockType);
    ColumnBlockDS ds;
    switch (cbt) {
      case RadixTrie: {
        ds = new RadixTrieColumnBlockDS();
        break;
      }
      case HashDict: {
        ds = new HashDictionaryColumnBlockDS();
        break;
      }
      case SortedHash: {
        ds = new SortedHashDictionaryColumnBlockDS();
        break;
      }
      default:
        throw new RuntimeException("Can't find column block storage data struct.");
    }
    ds.setDataType(colType);
//    ds.setNeedMarkIsModifiedByCrud(needMarkIsModifiedByCrud);
    return ds;
  }
}

