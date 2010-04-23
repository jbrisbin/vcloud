package com.jbrisbin.vcloud.session;

import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 10, 2010 Time: 11:02:21 AM To change this template use File |
 * Settings | File Templates.
 */
public class CloudSessionReplicationValve extends ValveBase {

  protected static final String info = "CloudSessionReplicationValve/1.0";

  @Override
  public String getInfo() {
    return info;
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
      ((CloudManager) manager).getStore().replicateSession( session );
    }
  }
}
