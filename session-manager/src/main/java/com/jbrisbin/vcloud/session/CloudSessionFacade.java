package com.jbrisbin.vcloud.session;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.util.Enumeration;

/**
 * Created by IntelliJ IDEA.
 * User: jbrisbin
 * Date: Apr 2, 2010
 * Time: 5:15:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class CloudSessionFacade implements HttpSession {

  private CloudSession session;

  public CloudSessionFacade( CloudSession session ) {
    this.session = session;
  }

  public long getCreationTime() {
    return session.getCreationTime();
  }

  public String getId() {
    return session.getId();
  }

  public long getLastAccessedTime() {
    return session.getLastAccessedTime();
  }

  public ServletContext getServletContext() {
    return session.getServletContext();
  }

  public void setMaxInactiveInterval( int i ) {
    session.setMaxInactiveInterval( i );
  }

  public int getMaxInactiveInterval() {
    session.setDirty( true );
    return session.getMaxInactiveInterval();
  }

  public HttpSessionContext getSessionContext() {
    return session.getSessionContext();
  }

  public Object getAttribute( String s ) {
    return session.getAttribute( s );
  }

  public Object getValue( String s ) {
    return session.getValue( s );
  }

  public Enumeration getAttributeNames() {
    return session.getAttributeNames();
  }

  public String[] getValueNames() {
    return session.getValueNames();
  }

  public void setAttribute( String s, Object o ) {
    session.setAttribute( s, o );
    session.setDirty( true );
  }

  public void putValue( String s, Object o ) {
    session.putValue( s, o );
    session.setDirty( true );
  }

  public void removeAttribute( String s ) {
    session.removeAttribute( s );
    session.setDirty( true );
  }

  public void removeValue( String s ) {
    session.removeValue( s );
    session.setDirty( true );
  }

  public void invalidate() {
    session.invalidate();
    session.setDirty( true );
  }

  public boolean isNew() {
    return session.isNew();
  }
}
