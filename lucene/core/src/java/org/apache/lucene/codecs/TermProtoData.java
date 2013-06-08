package org.apache.lucene.codecs;

import org.apache.lucene.index.TermState; 

public class TermProtoData {
  // customize by term dict
  public TermState state;

  // customize by postings base format
  public TermMetaData meta;

  public TermProtoData() {
    state = null;
    meta = null;
  }
  public TermProtoData(TermState state, TermMetaData meta) {
    this.state = state;
    this.meta= meta;
  }
  public void setMetaData(TermMetaData meta) {
    this.meta = meta;
  }
  public void setState(TermState state) {
    this.state = state;
  }
  public TermMetaData getMetaData() {
    return meta;
  }
  public TermState getState() {
    return state;
  }

  @Override
  public String toString() {
    return "TermProtoData";
  }
}
