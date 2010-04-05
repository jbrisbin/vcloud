package com.jbrisbin.vcloud.session;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by IntelliJ IDEA.
 * User: jbrisbin
 * Date: Apr 2, 2010
 * Time: 5:04:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class CloudSession extends StandardSession {

  public static enum Events {
    CREATE, DESTROY, UPDATE, LOAD, CLEAR, REPLICATE
  }

  private AtomicBoolean dirty = new AtomicBoolean( false );

  public CloudSession( Manager manager ) {
    super( manager );
  }

  public void setDirty( boolean dirty ) {
    this.dirty.set( dirty );
  }

  public boolean isDirty() {
    return this.dirty.get();
  }

}
