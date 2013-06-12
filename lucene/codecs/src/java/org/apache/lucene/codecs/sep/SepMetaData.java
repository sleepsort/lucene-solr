package org.apache.lucene.codecs.sep;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.IOUtils;

// nocommit: not actually implemented !! This file is just written
// to pass compilation. We should refactor pulsing codec 
// separately first, before changing to this.
final class SepMetaData extends TermMetaData {
  IntIndexInput.Index docIndex;
  IntIndexInput.Index freqIndex;
  IntIndexInput.Index posIndex;
  long payloadFP;
  long skipFP;

  byte[] bytes;
  ByteArrayDataInput bytesReader;

  public SepMetaData() {
  }
  public SepMetaData clone() {
    SepMetaData other = new SepMetaData();
    other.copyFrom(this);
    return other;
  }
  public void copyFrom(SepMetaData other) {
    if (docIndex == null) {
      docIndex = other.docIndex.clone();
    } else {
      docIndex.copyFrom(other.docIndex);
    }
    if (other.freqIndex != null) {
      if (freqIndex == null) {
        freqIndex = other.freqIndex.clone();
      } else {
        freqIndex.copyFrom(other.freqIndex);
      }
    } else {
      freqIndex = null;
    }
    if (other.posIndex != null) {
      if (posIndex == null) {
        posIndex = other.posIndex.clone();
      } else {
        posIndex.copyFrom(other.posIndex);
      }
    } else {
      posIndex = null;
    }
    payloadFP = other.payloadFP;
    skipFP = other.skipFP;
  }
}
