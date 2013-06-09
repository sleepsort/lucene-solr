package org.apache.lucene.codecs;

import org.apache.lucene.index.TermState; 

public class TermProtoData {
  // customize by term dict
  // exposing docFreq, totalTermFreq to PBF
  public BlockTermState state;

  // customize by postings base format
  public TermMetaData meta;

  public TermProtoData() {
    state = null;
    meta = null;
  }
  public TermProtoData(BlockTermState state, TermMetaData meta) {
    this.state = state;
    this.meta= meta;
  }
  public void setMetaData(TermMetaData meta) {
    this.meta = meta;
  }
  public void setState(BlockTermState state) {
    this.state = state;
  }
  public TermMetaData getMetaData() {
    return meta;
  }
  public BlockTermState getState() {
    return state;
  }

  @Override
  public String toString() {
    return "TermProtoData";
  }
}
