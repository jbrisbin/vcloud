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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.util.Enumeration;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 2, 2010 Time: 5:15:43 PM To change this template use File |
 * Settings | File Templates.
 */
@SuppressWarnings({"deprecation"})
public class CloudSessionFacade implements HttpSession {

  private CloudSession session;

  public CloudSessionFacade(CloudSession session) {
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

  public void setMaxInactiveInterval(int i) {
    session.setMaxInactiveInterval(i);
  }

  public int getMaxInactiveInterval() {
    return session.getMaxInactiveInterval();
  }

  public HttpSessionContext getSessionContext() {
    return session.getSessionContext();
  }

  public Object getAttribute(String s) {
    return session.getAttribute(s);
  }

  public Object getValue(String s) {
    return session.getValue(s);
  }

  public Enumeration getAttributeNames() {
    return session.getAttributeNames();
  }

  public String[] getValueNames() {
    return session.getValueNames();
  }

  public void setAttribute(String s, Object o) {
    session.setAttribute(s, o);
  }

  public void putValue(String s, Object o) {
    session.putValue(s, o);
  }

  public void removeAttribute(String s) {
    session.removeAttribute(s);
  }

  public void removeValue(String s) {
    session.removeValue(s);
  }

  public void invalidate() {
    session.invalidate();
  }

  public boolean isNew() {
    return session.isNew();
  }
}
