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

import org.apache.catalina.Session;
import org.apache.catalina.session.StandardSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 6, 2010 Time: 8:34:08 AM To change this template use File |
 * Settings | File Templates.
 */
public class InternalSessionSerializer implements SessionSerializer {

  protected Session session;

  public Session getSession() {
    return this.session;
  }

  public void setSession(Session session) {
    if (session instanceof StandardSession) {
      this.session = session;
    } else {
      throw new IllegalArgumentException("This serializer can only handle StandardSession objects (and subclasses).");
    }
  }

  public byte[] serialize() throws IOException {

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);

    ((StandardSession) session).writeObjectData(objectOut);

    objectOut.flush();
    objectOut.close();
    bytesOut.flush();
    bytesOut.close();

    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }
}
