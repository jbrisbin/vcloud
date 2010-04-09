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

import javax.servlet.http.HttpSession;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 3, 2010 Time: 11:10:33 AM To change this template use File |
 * Settings | File Templates.
 */
@SuppressWarnings({"unchecked"})
public class CloudManager extends ManagerBase implements Lifecycle, LifecycleListener, PropertyChangeListener {

  protected static final String info = "CloudManager/1.0";
  protected static final String name = "CloudManager";

  protected Log log = LogFactory.getLog(getClass());
  protected CloudStore store;
  protected LifecycleSupport lifecycle = new LifecycleSupport(this);
  protected PropertyChangeSupport propertyChange = new PropertyChangeSupport(this);
  protected AtomicBoolean started = new AtomicBoolean(false);
  protected AtomicInteger rejectedSessions = new AtomicInteger(0);

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
    if (log.isDebugEnabled()) {
      log.debug("processExpires()");
    }
  }

  @Override
  public void add(Session session) {
    if (log.isDebugEnabled()) {
      log.debug("add(): " + session.toString());
    }
    try {
      store.save(session);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
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
      sessionCounter++;
    } else {
      session.setId(sessionId);
    }

    return session;
  }

  @Override
  public Session createEmptySession() {
    return getNewSession();
  }

  @Override
  public Session findSession(String id) throws IOException {
    Session session = null;
    if (store.getLocalSessions().containsKey(id)) {
      session = store.getLocalSessions().get(id);
    }
    if (null == session) {
      // Try to find it somewhere in the cloud
      try {
        session = store.load(id);
        if (null != session) {
          if (!session.isValid()) {
            session.expire();
            remove(session);
            return null;
          }

          session.setManager(this);
          ((StandardSession) session).tellNew();
          add(session);
          session.endAccess();
        }
      } catch (ClassNotFoundException e) {
        log.error(e.getMessage(), e);
      }
    }

    return session;
  }

  @Override
  public Session[] findSessions() {
    String[] ids = store.getCloudSessionIds();
    List<Session> sessions = new ArrayList<Session>();
    for (String id : ids) {
      try {
        Session sess = store.load(id);
        if (null != sess) {
          sessions.add(sess);
        }
      } catch (ClassNotFoundException e) {
        log.error(e.getMessage(), e);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
    return sessions.toArray(new Session[sessions.size()]);
  }

  @Override
  public void remove(Session session) {
    try {
      store.remove(session.getId());
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  protected StandardSession getNewSession() {
    return new CloudSession(this);
  }

  @Override
  public String listSessionIds() {
    StringBuffer buff = new StringBuffer();
    boolean needsComma = false;
    for (String id : store.getCloudSessionIds()) {
      if (needsComma) {
        buff.append(", ");
      } else {
        needsComma = true;
      }
      buff.append(id);
    }
    return buff.toString();
  }

  @Override
  public String getSessionAttribute(String sessionId, String key) {
    Session session = null;
    try {
      session = findSession(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if (null != session) {
      Object o = session.getSession().getAttribute(key);
      if (o instanceof String) {
        return (String) o;
      } else {
        return (null != o ? o.toString() : null);
      }
    }

    return null;
  }

  @Override
  public HashMap getSession(String sessionId) {
    Session session = null;
    try {
      session = findSession(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if (null != session) {
      HttpSession httpSession = session.getSession();
      HashMap map = new HashMap();
      String key = null;
      for (Enumeration keys = httpSession.getAttributeNames(); keys.hasMoreElements(); key = keys.nextElement()
          .toString()) {
        map.put(key, httpSession.getAttribute(key));
      }
      return map;
    }
    return new HashMap();
  }

  @Override
  public void expireSession(String sessionId) {
    try {
      store.remove(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public long getLastAccessedTimestamp(String sessionId) {
    Session session = null;
    try {
      session = findSession(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if (null != session) {
      return session.getLastAccessedTime();
    }
    return -1L;
  }

  @Override
  public String getLastAccessedTime(String sessionId) {
    Session session = null;
    try {
      session = findSession(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if (null != session) {
      return String.valueOf(session.getLastAccessedTime());
    }
    return null;
  }

  @Override
  public String getCreationTime(String sessionId) {
    Session session = null;
    try {
      session = findSession(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if (null != session) {
      return String.valueOf(session.getCreationTime());
    }
    return null;
  }

  @Override
  public long getCreationTimestamp(String sessionId) {
    Session session = null;
    try {
      session = findSession(sessionId);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    if (null != session) {
      return session.getCreationTime();
    }
    return -1L;
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
    if (log.isDebugEnabled()) {
      log.debug("manager.start()");
    }
    if (started.get()) {
      return;
    }
    lifecycle.fireLifecycleEvent(START_EVENT, null);
    try {
      init();
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
    store.start();
    started.set(true);
  }

  public void stop() throws LifecycleException {
    if (log.isDebugEnabled()) {
      log.debug("manager.stop()");
    }
    started.set(false);
    lifecycle.fireLifecycleEvent(STOP_EVENT, null);
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
        log.error(nfe.getMessage(), nfe);
      }
    }
  }

  public int getRejectedSessions() {
    return rejectedSessions.get();
  }

  public void setRejectedSessions(int i) {
    rejectedSessions.set(i);
  }

  public void load() throws ClassNotFoundException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("load()");
    }
  }

  public void unload() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("unload()");
    }
  }
}
