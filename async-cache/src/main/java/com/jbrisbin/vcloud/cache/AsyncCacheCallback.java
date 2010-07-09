package com.jbrisbin.vcloud.cache;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface AsyncCacheCallback {

  public void onObjectLoad( Object obj );

  public void onError( Throwable t );

}
