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

package com.jbrisbin.vcloud.cache;

import java.io.*;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class ObjectMessage {

  String id;
  String exchange;
  String routingKey;
  byte[] body;
  Object obj;

  public ObjectMessage( String id, String exchange, String routingKey,
                        byte[] body ) throws IOException, ClassNotFoundException {
    this.id = id;
    this.exchange = exchange;
    this.routingKey = routingKey;
    setBody( body );
  }

  public ObjectMessage( String id, String exchange, String routingKey,
                        Object obj ) throws IOException {
    this.id = id;
    this.exchange = exchange;
    this.routingKey = routingKey;
    setObject( obj );
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public String getExchange() {
    return exchange;
  }

  public void setExchange( String exchange ) {
    this.exchange = exchange;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public void setRoutingKey( String routingKey ) {
    this.routingKey = routingKey;
  }

  public byte[] getBody() {
    return body;
  }

  public void setBody( byte[] body ) throws ClassNotFoundException, IOException {
    this.body = body;
    this.obj = deserialize( body );
  }

  public Object getObject() {
    return this.obj;
  }

  public void setObject( Object obj ) throws IOException {
    this.obj = obj;
    body = serialize( obj );
  }

  public static byte[] serialize( Object obj ) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream( bytesOut );
    oout.writeObject( obj );
    oout.flush();
    oout.close();
    bytesOut.flush();
    bytesOut.close();
    return bytesOut.toByteArray();
  }

  public static Object deserialize( byte[] bytes ) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream( bytes );
    ObjectInputStream oin = new ObjectInputStream( bytesIn );
    Object obj = oin.readObject();
    oin.close();
    bytesIn.close();
    return obj;
  }

}
