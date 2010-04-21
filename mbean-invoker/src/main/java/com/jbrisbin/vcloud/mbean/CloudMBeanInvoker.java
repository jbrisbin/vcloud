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

  public void setAttributeName( String name );

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
