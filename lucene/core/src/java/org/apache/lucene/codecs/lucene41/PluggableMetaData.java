package org.apache.lucene.codecs.lucene41;

import static org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene41.ForUtil.MAX_DATA_SIZE;
import static org.apache.lucene.codecs.lucene41.ForUtil.MAX_ENCODED_SIZE;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PluggablePostingsReaderBase;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.codecs.TermProtoData;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

final class PluggableMetaData extends TermMetaData {
  long payStartFP;
  long skipOffset;
  long lastPosBlockOffset;
  // docid when there is a single pulsed posting, otherwise -1
  // freq is always implicitly totalTermFreq in this case.
  int singletonDocID;

  // NOTE: Only used for reader side
  // NOTE: Only used by the "primary" MetaData -- clones don't
  // copy this (basically they are "transient"):
  ByteArrayDataInput bytesReader;  // TODO: should this NOT be in the TermState...?
  byte[] bytes;

  public PluggableMetaData(long docStartFP, long posStartFP, long payStartFP, long skipOffset, long lastPosBlockOffset, int singletonDocID) {
    super(posStartFP != -1 ? 3 : 1, posStartFP != -1 ? 20 : 0);
    setDocFP(docStartFP);
    setPosFP(posStartFP);
    setPayFP(payStartFP);
    setSkipOffset(skipOffset);
    setLastPosBlockOffset(lastPosBlockOffset);
    setSingletonDocID(singletonDocID);
  }
  public PluggableMetaData() {
  }

  public void setSkipOffset(long skipOffset) {
    buffer.putLong(0, skipOffset);
  }
  public void setLastPosBlockOffset(long lastPosBlockOffset) {
    buffer.putLong(8, lastPosBlockOffset);
  }
  public void setSingletonDocID(int singletonDocID) {
    buffer.putInt(16, singletonDocID);
  }
  public void setDocFP(long docFP) {
    base.longs[0] = docFP;
  }
  public void setPosFP(long posFP) {
    base.longs[1] = posFP;
  }
  public void setPayFP(long payFP) {
    base.longs[2] = payFP;
  }

  public long skipOffset() {
    return buffer.getLong(0);
  }
  public long lastPosBlockOffset() {
    return buffer.getLong(8);
  }
  public int singletonDocID() {
    return buffer.getInt(16);
  }
  public long docFP() {
    return base.longs[0];
  }
  public long posFP() {
    return base.longs[1];
  }
  public long payFP() {
    return base.longs[2];
  }


  /*
  public PluggableMetaData clone() {
    PluggableMetaData other = new PluggableMetaData();
    other.copyFrom(this);
    return other;
  }

  public void copyFrom(TermMetaData _other) {
    PluggableMetaData other = (PluggableMetaData) _other;
    this.base = LongsRef.deepCopyOf(other.base);
    this.extend = BytesRef.deepCopyOf(other.extend);

    // Do not copy buffered postings bytes & bytesReader 
    // (else TermMetaData is very heavy, ie drags around the 
    // entire block's byte[]).  On seek back, if next() is in fact 
    // used (rare!), they will be re-read from disk.
  }*/

  @Override
  public String toString() {
    return super.toString() + " docStartFP=" + docFP() + " posStartFP=" + posFP() + " payStartFP=" + payFP() + " lastPosBlockOffset=" + lastPosBlockOffset() + " singletonDocID=" + singletonDocID();
  }
}
