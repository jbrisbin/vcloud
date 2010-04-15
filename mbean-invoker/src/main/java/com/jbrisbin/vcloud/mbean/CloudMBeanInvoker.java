package com.jbrisbin.vcloud.mbean;

import javax.management.MalformedObjectNameException;
import java.util.concurrent.Callable;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface CloudMBeanInvoker extends Callable<Object> {

  public void setName( String name ) throws MalformedObjectNameException;

  public String getName();

  public void setOperation( String name );

  public String getOperation();

  public void setAttributeName(String name);

  public String getAttributeName();

  public void setCorrelationId( String id );

  public String getCorrelationId();

  public void setReplyTo( String replyTo );

  public String getReplyTo();

  public void setArgs( Object[] args );

  public Object[] getArgs();

  public void setArgTypes( String[] argTypes );

  public String[] getArgTypes();

  public Object getAttributeValue( String attributeName );

  public Object invoke();
}
