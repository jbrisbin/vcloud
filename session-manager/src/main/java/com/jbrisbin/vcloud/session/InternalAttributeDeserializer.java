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

import org.apache.catalina.util.CustomObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class InternalAttributeDeserializer implements AttributeDeserializer {

  private Logger log = LoggerFactory.getLogger(getClass());
  private byte[] bytes;
  private Object obj = null;
  private ClassLoader classLoader;

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public Object deserialize() throws IOException, ClassNotFoundException {
    if (null == obj) {
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
      ObjectInputStream objectIn = (null != classLoader ? new CustomObjectInputStream(bytesIn,
          classLoader) : new ObjectInputStream(bytesIn));
      obj = objectIn.readObject();
    }
    return obj;
  }
}
