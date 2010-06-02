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

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;

/**
 * A custom implementation of the Tomcat <b>StandardSession</b> which adds some convenience features like a dirty flag
 * and an enum for the differen event types.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class CloudSession extends StandardSession {


  /**
   * Events related to sessions can be of several different types.
   */
  public static enum Events {
    TOUCH, DESTROY, UPDATE, LOAD, CLEAR, REPLICATE, SETATTR, DELATTR, GETALL
  }

  public static Events asEvent(String s) {
    return Events.valueOf(s.toUpperCase());
  }

  /**
   * Is this session a copy of another one somewhere in the cloud?
   */
  private boolean replica = false;

  public CloudSession(Manager manager) {
    super(manager);
  }

  public boolean isReplica() {
    return replica;
  }

  public synchronized void setReplica(boolean replica) {
    this.replica = replica;
  }

  @Override
  public void setAttribute(String name, Object value) {
    boolean needsReplicated = needsReplicated(name, value);
    super.setAttribute(name, value);
    if (needsReplicated) {
      replicateAttribute(name);
    }
  }

  @Override
  public void setAttribute(String name, Object value, boolean notify) {
    boolean needsReplicated = needsReplicated(name, value);
    super.setAttribute(name, value, notify);
    if (needsReplicated) {
      replicateAttribute(name);
    }
  }

  void setAttributeInternal(String name, Object value) {
    attributes.put(name, value);
  }

  @Override
  public void setPrincipal(Principal principal) {
    super.setPrincipal(principal);
    replicate();
  }

  void setPrincipalInternal(Principal principal) {
    super.setPrincipal(principal);
  }

  @Override
  public void removeAttribute(String name) {
    super.removeAttribute(name);
    replicateRemoveAttribute(name);
  }

  @Override
  public void removeAttribute(String name, boolean notify) {
    super.removeAttribute(name, notify);
    replicateRemoveAttribute(name);
  }

  void removeAttributeInternal(String name) {
    attributes.remove(name);
  }

  @Override
  public boolean isValid() {
    if (this.expiring) {
      return true;
    }
    if (!this.isValid) {
      LoggerFactory.getLogger(getClass()).debug(getIdInternal() + " isValid is false");
      return false;
    }
    if (ACTIVITY_CHECK && accessCount.get() > 0) {
      return true;
    }
    if (maxInactiveInterval >= 0) {
      long timeNow = System.currentTimeMillis();
      int timeIdle = (int) ((timeNow - thisAccessedTime) / 1000L);
      if (timeIdle >= maxInactiveInterval) {
        LoggerFactory.getLogger(getClass())
            .debug(String.format("%s timeIdle (%s) >= maxInactiveInterval (%s)",
                getIdInternal(),
                timeIdle,
                maxInactiveInterval));
        expire(true);
      }
    }

    return (this.isValid);
  }

  @Override
  public String toString() {
    return "CloudSession[" + getIdInternal() + "]";
  }

  /**
   * Return a boolean indicating whether or not this attribute needs replicated.
   *
   * @param name
   * @param obj
   * @return
   */
  protected boolean needsReplicated(String name, Object obj) {
    if (attributes.containsKey(name)) {
      Object orig = attributes.get(name);
      if (null != obj && obj.equals(orig)) {
        return true;
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  protected void replicate() {
    try {
      getStore().replicateSession(this);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void replicateRemoveAttribute(String attr) {
    try {
      getStore().removeAttribute(this, attr);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void replicateAttribute(String attr) {
    try {
      getStore().replicateAttribute(this, attr);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected CloudStore getStore() {
    Manager mgr = getManager();
    if (mgr instanceof CloudManager) {
      return ((CloudManager) mgr).getStore();
    }
    return null;
  }
}
