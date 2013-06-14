package org.apache.lucene.codecs.temp;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;

import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;


// Must keep final because we do non-standard clone
final class TempMetaData extends TermMetaData {
  long docStartFP;
  long posStartFP;
  long payStartFP;
  long skipOffset;
  long lastPosBlockOffset;
  // docid when there is a single pulsed posting, otherwise -1
  // freq is always implicitly totalTermFreq in this case.
  int singletonDocID;

  /* no arg means the instance will be always 'less than' any other instance */
  TempMetaData() {
    docStartFP = 0;
    posStartFP = 0;
    payStartFP = 0;
    singletonDocID = 0;
  }

  TempMetaData(long docStartFP, long posStartFP, long payStartFP, long skipOffset, long lastPosBlockOffset, int singletonDocID) {
    this.docStartFP = docStartFP;
    this.posStartFP = posStartFP;
    this.payStartFP = payStartFP;
    this.skipOffset = skipOffset;
    this.lastPosBlockOffset = lastPosBlockOffset;
    this.singletonDocID = singletonDocID;
  }

  /* delta-encoding, only works on monotonical part */
  public TermMetaData subtract(TermMetaData _inc) {
    TempMetaData inc = (TempMetaData) _inc;
    TempMetaData ret = (TempMetaData)super.clone();
    if (ret.singletonDocID != -1) {  // current MetaData have no valid docFP, copy it so delta=0
      ret.docStartFP = 0;
    } else {
      assert inc.docStartFP <= ret.docStartFP;
      ret.docStartFP -= inc.docStartFP;
    }
    if (ret.posStartFP != -1) {
      assert inc.posStartFP <= ret.posStartFP;
      ret.posStartFP -= inc.posStartFP;
    }
    if (ret.payStartFP != -1 && inc.payStartFP != -1) {
      assert inc.payStartFP <= ret.payStartFP;
      ret.payStartFP -= inc.payStartFP;
    }
    return ret;
  }

  @Override
  public TermMetaData add(TermMetaData _inc) {
    TempMetaData inc = (TempMetaData) _inc;
    TempMetaData ret = (TempMetaData)super.clone();
    if (ret.singletonDocID != -1) {
      ret.docStartFP = inc.docStartFP;
    } else {
      ret.docStartFP += inc.docStartFP;
    }
    if (ret.posStartFP != -1) {
      ret.posStartFP += inc.posStartFP;
    }
    // nocommit: not symmetric with those lines in subtract... ,
    // the deep reason is: during writing, we depends on the '-1' values 
    // to see whether to write them...
    //
    // Here, the use of subtract() & add() are quite different:
    // subtract() will get the delta value to *write* to disk, 
    //            so the format must be exactly the same as a pendingTerm in 
    //            Lucene41PostingsWriter.flushTermBlock()
    // add() will try to pass the 'lastPayStartFP' along the consumed metadata,
    //       so the metadata returned by 'add' will *never* be written to disk.
    // for example, whether we have payStartFP depends on the totalTermFreq of a term,
    // like for term A,B,C, A & C has payStartFP, but B doesn't, so, 
    // the payStartFP returned by B.subtract(A) should be -1, otherwise the write will stupidly write another VLong,
    // the payStartFP returned by B.subtract(A).add(A) should be the same as A
    //
    if (inc.payStartFP != -1) {
      if (ret.payStartFP != -1) {
        ret.payStartFP += inc.payStartFP;
      } else {
        ret.payStartFP = inc.payStartFP;
      }
    }
    return ret;
  }

  @Override
  // nocommit: should TermState be glued with field info? quite stupid to calculate indexOptions here
  // but we're silly calculating this in Lucenen41PSTR as well..
  public void write(DataOutput out, FieldInfo info, TempTermState state) throws IOException {
    final IndexOptions indexOptions = info.getIndexOptions();
    boolean fieldHasFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean fieldHasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean fieldHasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean fieldHasPayloads = info.hasPayloads();

    if (singletonDocID == -1) {
      out.writeVLong(docStartFP);
    } else {
      out.writeVInt(singletonDocID);
    }

    if (fieldHasPositions) {
      out.writeVLong(posStartFP);
      if (lastPosBlockOffset != -1) {
        out.writeVLong(lastPosBlockOffset);
      }
      if ((fieldHasPayloads || fieldHasOffsets) && payStartFP != -1) {
        out.writeVLong(payStartFP);
      }
    }
    if (skipOffset != -1) {
      out.writeVLong(skipOffset);
    }
  }

  @Override
  public void read(DataInput in, FieldInfo info, TempTermState state) throws IOException {
  }

  @Override
  public TempMetaData clone() {
    TempMetaData other = new TempMetaData();
    other.copyFrom(this);
    return other;
  }

  @Override
  public void copyFrom(TermMetaData _other) {
    //super.copyFrom(_other);
    TempMetaData other = (TempMetaData) _other;
    docStartFP = other.docStartFP;
    posStartFP = other.posStartFP;
    payStartFP = other.payStartFP;
    lastPosBlockOffset = other.lastPosBlockOffset;
    skipOffset = other.skipOffset;
    singletonDocID = other.singletonDocID;
  }

  @Override
  public String toString() {
    return "docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
  }
}
