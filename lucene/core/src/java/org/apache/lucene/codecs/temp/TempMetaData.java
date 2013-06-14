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

import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.store.ByteArrayDataInput;


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

  TempMetaData() {
  }

  TempMetaData(long docStartFP, long posStartFP, long payStartFP, long skipOffset, long lastPosBlockOffset, int singletonDocID) {
    this.docStartFP = docStartFP;
    this.posStartFP = posStartFP;
    this.payStartFP = payStartFP;
    this.skipOffset = skipOffset;
    this.lastPosBlockOffset = lastPosBlockOffset;
    this.singletonDocID = singletonDocID;
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
    return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
  }
}
