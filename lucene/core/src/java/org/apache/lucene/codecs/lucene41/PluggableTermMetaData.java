package org.apache.lucene.codecs.lucene41;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.codecs.TermValues;

final class PluggableTermMetaData extends TermMetaData {
  public static int IDX_DOC_FP = 0;
  public static int IDX_POS_FP = 1;
  public static int IDX_PAY_FP = 2;

  public static int SZ_SKIP_OFFSET = 8;
  public static int SZ_LAST_POS_BLOCK_OFFSET = 8;
  public static int SZ_SINGLETON_DOC_ID = 4;
  
  public static int SZ_BYTES = SZ_SKIP_OFFSET + SZ_LAST_POS_BLOCK_OFFSET + SZ_SINGLETON_DOC_ID;

  PluggableTermMetaData(int baseLength, int extendLength) {
    super(new LongsRef(baseLength), new BytesRef(extendLength));
  }

  @Override
  public void write(DataOutput out, TermValues values) throws IOException {
    long skipOffset;
    long lastPosBlockOffset;
    int singletonDocID;
 
    for (int i = base.offset; i < base.offset + base.length; i++) {
      out.writeVLong(base.longs[i]);
    }

  }

  @Override
  public void read(DataInput in, TermValues values) throws IOException {
  }

  @Override
  public String toString() {
    return "Pluggable: "+base.toString() + " " + extend.toString();
  }
}
