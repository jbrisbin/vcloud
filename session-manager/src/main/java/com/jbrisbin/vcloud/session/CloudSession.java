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
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 2, 2010 Time: 5:04:00 PM To change this template use File |
 * Settings | File Templates.
 */
public class CloudSession extends StandardSession {

  public static enum Events {
    CREATE, DESTROY, UPDATE, LOAD, CLEAR, REPLICATE
  }

  private AtomicBoolean dirty = new AtomicBoolean(false);

  public CloudSession(Manager manager) {
    super(manager);
  }

  public void setDirty(boolean dirty) {
    this.dirty.set(dirty);
  }

  public boolean isDirty() {
    return this.dirty.get();
  }

  @Override
  public String toString() {
    return "CloudSession[" + getIdInternal() + "]";
  }
}
