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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A custom implementation of the Tomcat <b>StandardSession</b> which adds some convenience features like a dirty flag
 * and an enum for the differen event types.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class CloudSession extends StandardSession {
  /**
   * Events related to sessions can be of several different types.
   */
  public static enum Events {
    TOUCH, DESTROY, UPDATE, LOAD, CLEAR, REPLICATE, GETIDS, GETALL
  }

  /**
   * Whether anything has changed in this session, which might be useful for replication code.
   */
  private AtomicBoolean dirty = new AtomicBoolean(false);
  /**
   * Is this session a copy of another one somewhere in the cloud?
   */
  private boolean replica = false;

  public CloudSession(Manager manager) {
    super(manager);
  }

  public void setDirty(boolean dirty) {
    this.dirty.set(dirty);
  }

  public boolean isDirty() {
    return this.dirty.get();
  }

  public boolean isReplica() {
    return replica;
  }

  public synchronized void setReplica(boolean replica) {
    this.replica = replica;
  }

  @Override
  public String toString() {
    return "CloudSession[" + getIdInternal() + "]";
  }
}
