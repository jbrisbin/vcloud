/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.jbrisbin.vcloud.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class InternalAttributeSerializer implements AttributeSerializer {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private Object obj;

  public void setObject(Object obj) {
    this.obj = obj;
  }

  public byte[] serialize() {
    if (null != obj) {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      try {
        ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
        objectOut.writeObject(obj);
        objectOut.flush();
        objectOut.close();
        bytesOut.flush();
        bytesOut.close();
        byte[] bytes = bytesOut.toByteArray();

        return bytes;
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
    return null;
  }
}
