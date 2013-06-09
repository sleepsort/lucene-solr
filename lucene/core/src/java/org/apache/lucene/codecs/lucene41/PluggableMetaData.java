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
  long docStartFP;
  long posStartFP;
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
    this.docStartFP = docStartFP;
    this.posStartFP = posStartFP;
    this.payStartFP = payStartFP;
    this.skipOffset = skipOffset;
    this.lastPosBlockOffset = lastPosBlockOffset;
    this.singletonDocID = singletonDocID;
  }
  public PluggableMetaData() {
  }

  public PluggableMetaData clone() {
    PluggableMetaData other = new PluggableMetaData();
    other.copyFrom(this);
    return other;
  }

  public void copyFrom(TermMetaData _other) {
    PluggableMetaData other = (PluggableMetaData) _other;
    docStartFP = other.docStartFP;
    posStartFP = other.posStartFP;
    payStartFP = other.payStartFP;
    lastPosBlockOffset = other.lastPosBlockOffset;
    skipOffset = other.skipOffset;
    singletonDocID = other.singletonDocID;

    // Do not copy bytes, bytesReader (else TermMetaData is
    // very heavy, ie drags around the entire block's
    // byte[]).  On seek back, if next() is in fact used
    // (rare!), they will be re-read from disk.
  }

  @Override
  public String toString() {
    return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
  }
}
