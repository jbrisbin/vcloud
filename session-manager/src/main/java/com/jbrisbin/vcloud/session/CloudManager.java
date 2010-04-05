package com.jbrisbin.vcloud.session;

import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: jbrisbin
 * Date: Apr 3, 2010
 * Time: 11:10:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class CloudManager extends ManagerBase implements LifecycleListener, PropertyChangeListener {

  private static final String info = "CloudManager/1.0";
  private static final String name = "CloudManager";

  private CloudStore store;

  @Override
  public String getInfo() {
    return info;
  }

  @Override
  public String getName() {
    return name;
  }

  public CloudStore getStore() {
    return store;
  }

  public void setStore( CloudStore store ) {
    this.store = store;
    store.setManager( this );
  }

  @Override
  public void processExpires() {

  }

  @Override
  public void add( Session session ) {

  }

  @Override
  public Session createSession( String sessionId ) {
    Session session = createEmptySession();
    session.setNew( true );
    session.setValid( true );
    session.setCreationTime( System.currentTimeMillis() );
    session.setMaxInactiveInterval( this.maxInactiveInterval );
    if ( null == sessionId ) {
      session.setId( generateSessionId() );
    }
    sessionCounter++;
    return session;
  }

  @Override
  public Session createEmptySession() {
    return getNewSession();
  }

  @Override
  public Session findSession( String id ) throws IOException {
    if ( store.getLocalSessions().containsKey( id ) ) {
      return store.getLocalSessions().get( id );
    } else {
      return null;
    }
  }

  @Override
  public Session[] findSessions() {
    return store.getLocalSessions().
  }

  @Override
  public void remove( Session session ) {
    super.remove( session );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  protected StandardSession getNewSession() {
    return new CloudSession( this );
  }

  @Override
  public String listSessionIds() {
    return super.listSessionIds();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public String getSessionAttribute( String sessionId, String key ) {
    return super.getSessionAttribute( sessionId,
        key );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public HashMap getSession( String sessionId ) {
    return super
        .getSession( sessionId );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void expireSession( String sessionId ) {
    super.expireSession( sessionId );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public long getLastAccessedTimestamp( String sessionId ) {
    return super.getLastAccessedTimestamp(
        sessionId );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public String getLastAccessedTime( String sessionId ) {
    return super.getLastAccessedTime(
        sessionId );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public String getCreationTime( String sessionId ) {
    return super
        .getCreationTime( sessionId );    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public long getCreationTimestamp( String sessionId ) {
    return super.getCreationTimestamp(
        sessionId );    //To change body of overridden methods use File | Settings | File Templates.
  }

  public void lifecycleEvent( LifecycleEvent lifecycleEvent ) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public int getRejectedSessions() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void setRejectedSessions( int i ) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public void load() throws ClassNotFoundException, IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public void unload() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
