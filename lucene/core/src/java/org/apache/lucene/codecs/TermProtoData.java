package org.apache.lucene.codecs;

import org.apache.lucene.index.TermState; 

public class TermProtoData {
  // shared
  TermValues values;
 
  // customize by term dict
  TermState state;

  // customize by postings base format
  TermMetaData meta;

  public TermProtoData() {
    values = null;
    state = null;
    meta = null;
  }
  public TermProtoData(TermValues values, TermState state, TermMetaData meta) {
    this.values = values;
    this.state = state;
    this.meta= meta;
  }
  public void setValues(TermValues values) {
    this.values = values;
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
  public TermValues getValues() {
    return values;
  }

  @Override
  public String toString() {
    return "TermProtoData";
  }
}
