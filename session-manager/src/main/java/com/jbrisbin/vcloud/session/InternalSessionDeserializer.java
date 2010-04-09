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
import org.apache.catalina.util.CustomObjectInputStream;
import org.apache.juli.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Re-constitute a user <b>StandardSession</b> subclass from a byte stream.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class InternalSessionDeserializer implements SessionDeserializer {

  /**
   * The serialized byte that likely came from a RabbitMQ message.
   */
  protected byte[] bytes;
  /**
   * Our <b>Session</b> object.
   */
  protected Session session;
  /**
   * The ClassLoader to use while deserializing our Session. This is required to handle any custom objects you put in
   * your session that aren't in the server's classpath.
   */
  protected ClassLoader classLoader = null;

  public byte[] getBytes() {
    return this.bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public Session getSession() {
    return this.session;
  }

  /**
   * The session should be created in the caller's context and passed to the deserializer. We DO NOT create one!
   *
   * @param session
   */
  public void setSession(Session session) {
    if (session instanceof StandardSession) {
      this.session = session;
    } else {
      throw new IllegalArgumentException("This serializer can only handle StandardSession objects (and subclasses).");
    }
  }

  /**
   * Turn our bytes into a real <b>Session</b> object.
   *
   * @return
   * @throws IOException
   */
  public Session deserialize() throws IOException {

    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
    ObjectInputStream objectIn = (null != classLoader ? new CustomObjectInputStream(bytesIn,
        classLoader) : new ObjectInputStream(bytesIn));
    try {
      ((StandardSession) session).readObjectData(objectIn);
    } catch (ClassNotFoundException e) {
      LogFactory.getLog(getClass()).error(e.getMessage(), e);
    }
    objectIn.close();
    bytesIn.close();

    return session;
  }
}
