package com.jbrisbin.vcloud.cache.test;

import java.io.Serializable;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class TestObject implements Serializable {

  private String data;

  public String getData() {
    return data;
  }

  public void setData( String data ) {
    this.data = data;
  }

  @Override
  public String toString() {
    String s = super.toString();
    return s + ",data=" + data.substring( 0, 16 ) + "...";
  }
}
