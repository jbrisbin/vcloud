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

import java.io.IOException;

/**
 * Generic interface for serializing sessions in case we want to do that differently later.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface SessionSerializer {

  public Session getSession();

  public void setSession(Session session);

  public byte[] serialize() throws IOException;

  public String getMD5Sum();
}
