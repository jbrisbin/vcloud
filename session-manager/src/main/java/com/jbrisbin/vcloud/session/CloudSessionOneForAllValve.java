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

import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class CloudSessionOneForAllValve extends ValveBase {

  protected static final String info = "CloudSessionOneForAllValve/1.0";

  @Override
  public String getInfo() {
    return this.info;
  }

  @Override
  public void invoke( Request request, Response response ) throws IOException, ServletException {
    getNext().invoke( request, response );

    Session session = null;
    try {
      session = request.getSessionInternal();
    } catch ( Throwable t ) {
      // IGNORED
    }
    if ( null != session ) {
      Manager manager = request.getContext().getManager();
      ((CloudManager) manager).getStore().processDeleteEvent( session.getId() );
    }

  }
}
