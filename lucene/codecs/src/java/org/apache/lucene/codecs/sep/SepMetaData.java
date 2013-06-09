package org.apache.lucene.codecs.sep;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.codecs.TermProtoData;
import org.apache.lucene.index.TermState;
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
    throw new IllegalStateException("not implemented");
  }
}
