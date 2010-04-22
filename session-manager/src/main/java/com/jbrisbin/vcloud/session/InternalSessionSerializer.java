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
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Convert's our <b>StandardSession</b> subclass into a byte array for inclusion in an MQ message.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class InternalSessionSerializer implements SessionSerializer {
  /**
   * The session to serialize.
   */
  protected Session session;
  protected byte[] bytes = null;
  protected String md5sum;

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

  /**
   * Serialize the session into a byte array.
   *
   * @return
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    if (null == bytes) {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);

      ((StandardSession) session).writeObjectData(objectOut);

      objectOut.flush();
      objectOut.close();
      bytesOut.flush();
      bytesOut.close();

      bytes = bytesOut.toByteArray();
      try {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(bytes);
        md5sum = new BigInteger(1, digest.digest()).toString(16);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    }
    return bytes;
  }

  public String getMD5Sum() {
    return md5sum;
  }
}
