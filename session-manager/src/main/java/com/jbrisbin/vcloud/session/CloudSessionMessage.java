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

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 3, 2010 Time: 9:21:56 AM To change this template use File |
 * Settings | File Templates.
 */
public class CloudSessionMessage {

  private String type;
  private String source;
  private String id;
  private byte[] body;
  private boolean forwarded = false;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public byte[] getBody() {
    return body;
  }

  public void setBody(byte[] body) {
    this.body = body;
  }

  public boolean isForwarded() {
    return forwarded;
  }

  public void setForwarded(boolean forwarded) {
    this.forwarded = forwarded;
  }

  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    buff.append("type=").append(type).append(",source=").append(source).append("id=").append(id);
    return buff.toString();
  }
}
