package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.codecs.TermProtoData;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.IOUtils;

final class Lucene40MetaData extends TermMetaData {
  // NOTE: Only used for reader side
  // NOTE: Only used by the "primary" MetaData -- clones don't
  // copy this (basically they are "transient"):
  ByteArrayDataInput bytesReader;  // TODO: should this NOT be in the TermState...?
  byte[] bytes;

  public Lucene40MetaData(long freqOffset, long skipOffset, long proxOffset) {
    super(proxOffset != -1 ? 2 : 1, 8);
    setFreqOffset(freqOffset);
    setProxOffset(proxOffset);
    setSkipOffset(skipOffset);
  }
  public Lucene40MetaData(FieldInfo info) {
  }

  public void setFreqOffset(long freqOffset) {
    base.longs[0] = freqOffset;
  }
  public void setProxOffset(long proxOffset) {
    base.longs[2] = proxOffset;
  }
  public void setSkipOffset(long skipOffset) {
    buffer.putLong(0, skipOffset);
  }
  public long freqOffset() {
    return base.longs[0];
  }
  public long proxOffset() {
    return base.longs[1];
  }
  public long skipOffset() {
    return buffer.getLong(0);
  }
}
