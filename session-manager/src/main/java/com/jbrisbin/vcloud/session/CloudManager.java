/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.jbrisbin.vcloud.session;

import org.apache.catalina.*;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 3, 2010 Time: 11:10:33 AM To change this template use File |
 * Settings | File Templates.
 */
public class CloudManager extends ManagerBase implements Lifecycle, LifecycleListener, PropertyChangeListener {

  protected static final String info = "CloudManager/1.0";
  protected static final String name = "CloudManager";

  protected Log log = LogFactory.getLog("vcloud");
  protected CloudStore store;
  protected LifecycleSupport lifecycle = new LifecycleSupport(this);
  protected PropertyChangeSupport propertyChange = new PropertyChangeSupport(this);
  protected AtomicBoolean started = new AtomicBoolean(false);

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

  public void setStore(Store store) {
    if (store instanceof CloudStore) {
      this.store = (CloudStore) store;
      store.setManager(this);
    }
  }

  @Override
  public void processExpires() {
    log.debug("processExpires()");
  }

  @Override
  public void add(Session session) {
    log.debug("add(): " + session.toString());
  }

  @Override
  public Session createSession(String sessionId) {
    Session session = createEmptySession();
    session.setNew(true);
    session.setValid(true);
    session.setCreationTime(System.currentTimeMillis());
    session.setMaxInactiveInterval(this.maxInactiveInterval);
    if (null == sessionId) {
      session.setId(generateSessionId());
    }
    try {
      store.save(session);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    sessionCounter++;
    return session;
  }

  @Override
  public Session createEmptySession() {
    return getNewSession();
  }

  @Override
  public Session findSession(String id) throws IOException {
    if (store.getLocalSessions().containsKey(id)) {
      return store.getLocalSessions().get(id);
    } else {
      // Try to find it somewhere in the cloud
      try {
        Session session = store.load(id);
        log.debug("Returning loaded session: " + id);
        return session;
      } catch (ClassNotFoundException e) {
        log.error(e.getMessage(), e);
      }
    }
    return null;
  }

  @Override
  public Session[] findSessions() {
    return null;
  }

  @Override
  public void remove(Session session) {
    super.remove(session);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  protected StandardSession getNewSession() {
    return new CloudSession(this);
  }

  @Override
  public String listSessionIds() {
    return super.listSessionIds();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public String getSessionAttribute(String sessionId, String key) {
    return super.getSessionAttribute(sessionId,
        key);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public HashMap getSession(String sessionId) {
    return super
        .getSession(sessionId);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void expireSession(String sessionId) {
    super.expireSession(sessionId);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public long getLastAccessedTimestamp(String sessionId) {
    return super.getLastAccessedTimestamp(
        sessionId);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public String getLastAccessedTime(String sessionId) {
    return super.getLastAccessedTime(
        sessionId);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public String getCreationTime(String sessionId) {
    return super
        .getCreationTime(sessionId);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public long getCreationTimestamp(String sessionId) {
    return super.getCreationTimestamp(
        sessionId);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void setContainer(Container container) {
    super.setContainer(container);
    log.debug("   ************** CONTAINER SET");
  }

  public void addLifecycleListener(LifecycleListener lifecycleListener) {
    lifecycle.addLifecycleListener(lifecycleListener);
  }

  public LifecycleListener[] findLifecycleListeners() {
    return lifecycle.findLifecycleListeners();
  }

  public void removeLifecycleListener(LifecycleListener lifecycleListener) {
    lifecycle.removeLifecycleListener(lifecycleListener);
  }

  public void lifecycleEvent(LifecycleEvent event) {
    log.debug(event.toString());
  }

  public void start() throws LifecycleException {
    log.debug("manager.start()");
    if (started.get()) {
      return;
    }
    lifecycle.fireLifecycleEvent(START_EVENT, null);
    started.set(true);

    try {
      init();
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }

    store.start();
  }

  public void stop() throws LifecycleException {
    log.debug("manager.stop()");
    lifecycle.fireLifecycleEvent(STOP_EVENT, null);
    started.set(false);
    store.stop();
  }

  public void propertyChange(PropertyChangeEvent propertyChangeEvent) {
    if (!(propertyChangeEvent.getSource() instanceof Context)) {
      // Ignore these
      return;
    }

    if (propertyChangeEvent.getPropertyName().equals("sessionTimeout")) {
      try {
        setMaxInactiveInterval(Integer.parseInt(propertyChangeEvent.getNewValue().toString()) * 60);
      } catch (NumberFormatException nfe) {
        getContainer().getLogger().error(nfe.getMessage(), nfe);
      }
    }
  }

  public int getRejectedSessions() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void setRejectedSessions(int i) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public void load() throws ClassNotFoundException, IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public void unload() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
