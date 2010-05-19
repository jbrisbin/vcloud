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

import com.rabbitmq.client.*;
import org.apache.catalina.*;
import org.apache.catalina.mbeans.MBeanUtils;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Provide a consumer and listener/handlers for invoking JMX operations via RabbitMQ messages.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class CloudInvokerListener implements ContainerListener, LifecycleListener {

  protected Log log = LogFactory.getLog(getClass());
  protected boolean DEBUG = log.isDebugEnabled();
  /**
   * Comma-delimited list of JMX domains to expose via messaging.
   */
  protected String exposeDomains = "Catalina";
  /**
   * Hostname of the RabbitMQ server we want to connect to.
   */
  protected String mqHost = "localhost";
  /**
   * RabbitMQ server's port number. Useful to set if running SSL or on a non-standard port for security reasons.
   */
  protected int mqPort = 5672;
  /**
   * A valid RabbitMQ user that has enough permissions to create Exchanges and Queues on the server.
   */
  protected String mqUser = "guest";
  /**
   * Plain-text password for the above user. It would be better to accept an encrypted password, but that's not a number
   * one priority, so I'll save that for later.
   */
  protected String mqPassword = "guest";
  /**
   * RabbitMQ virtual host to create our exchanges in. An entirely new "cluster" can be created by simply connecting to
   * a different virtual host.
   */
  protected String mqVirtualHost = "/";
  protected String instanceName = System.getenv("HOSTNAME");
  protected String eventsExchange = "amq.fanout";
  protected String eventsQueue = "events";
  protected String mbeanEventsExchange = "amq.topic";
  protected String mbeanEventsQueue = "events.mbean";
  protected String mbeanEventsRoutingKey = "#";
  protected Connection connection;
  protected Channel channel;
  protected BlockingQueue<QueueingConsumer.Delivery> incoming = new LinkedBlockingQueue<QueueingConsumer.Delivery>();
  protected ExecutorService workerPool = Executors.newCachedThreadPool();

  protected MBeanServer mbeanServer;

  public String getExposeDomains() {
    return exposeDomains;
  }

  public void setExposeDomains(String exposeDomains) {
    this.exposeDomains = exposeDomains;
  }

  public String getMqHost() {
    return mqHost;
  }

  public void setMqHost(String mqHost) {
    this.mqHost = mqHost;
  }

  public int getMqPort() {
    return mqPort;
  }

  public void setMqPort(int mqPort) {
    this.mqPort = mqPort;
  }

  public String getMqUser() {
    return mqUser;
  }

  public void setMqUser(String mqUser) {
    this.mqUser = mqUser;
  }

  public String getMqPassword() {
    return mqPassword;
  }

  public void setMqPassword(String mqPassword) {
    this.mqPassword = mqPassword;
  }

  public String getMqVirtualHost() {
    return mqVirtualHost;
  }

  public void setMqVirtualHost(String mqVirtualHost) {
    this.mqVirtualHost = mqVirtualHost;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  public String getEventsExchange() {
    return eventsExchange;
  }

  public void setEventsExchange(String eventsExchange) {
    this.eventsExchange = eventsExchange;
  }

  public String getEventsQueue() {
    return eventsQueue;
  }

  public void setEventsQueue(String eventsQueue) {
    this.eventsQueue = eventsQueue;
  }

  public String getMbeanEventsExchange() {
    return mbeanEventsExchange;
  }

  public void setMbeanEventsExchange(String mbeanEventsExchange) {
    this.mbeanEventsExchange = mbeanEventsExchange;
  }

  public String getMbeanEventsQueue() {
    return mbeanEventsQueue;
  }

  public void setMbeanEventsQueue(String mbeanEventsQueue) {
    this.mbeanEventsQueue = mbeanEventsQueue;
  }

  public String getMbeanEventsRoutingKey() {
    return mbeanEventsRoutingKey;
  }

  public void setMbeanEventsRoutingKey(String mbeanEventsRoutingKey) {
    this.mbeanEventsRoutingKey = mbeanEventsRoutingKey;
  }

  public void containerEvent(ContainerEvent event) {
    log.info("Type: " + event.getType().toString());
    try {
      log.info("Data: " + event.getData().toString());
    } catch (Throwable t) {
    }
  }

  public void lifecycleEvent(LifecycleEvent event) {
    if (null == mbeanServer) {
      mbeanServer = MBeanUtils.createServer();
    }
    if (null == connection) {
      try {
        ConnectionParameters params = new ConnectionParameters();
        params.setUsername(mqUser);
        params.setPassword(mqPassword);
        params.setVirtualHost(mqVirtualHost);
        if (DEBUG) {
          log.debug("Connecting to RabbitMQ server...");
        }
        connection = new ConnectionFactory(params).newConnection(mqHost, mqPort);
        channel = connection.createChannel();

        // For generic cloud events (membership, etc...)
        if (DEBUG) {
          log.debug("Declaring exch: " + eventsExchange + ", q: " + eventsQueue);
        }
        synchronized (channel) {
          //channel.exchangeDelete( eventsExchange );
          channel.exchangeDeclare(eventsExchange, "fanout", true);
          channel.queueDeclare(eventsQueue, false, true, false, true, null);
          channel.queueBind(eventsQueue, eventsExchange, "");
          // For mbean events
          if (DEBUG) {
            log.debug(
                "Declaring/binding exch: " + mbeanEventsExchange + ", q: " + mbeanEventsQueue + ", key: " + mbeanEventsRoutingKey);
          }
          //channel.exchangeDelete( mbeanEventsExchange );
          channel.exchangeDeclare(mbeanEventsExchange, "topic", true);
          channel.queueDeclare(mbeanEventsQueue, true);
          channel.queueBind(mbeanEventsQueue, mbeanEventsExchange, mbeanEventsRoutingKey);
        }
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

    // Let cloud know about this Lifecycle event
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    try {
      if (DEBUG) {
        log.debug("Attempting to notify cloud of " + event.getType() + " event...");
      }
      channel.basicPublish(eventsExchange, event.getType() + "." + instanceName, props, event.getType().getBytes());
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    if (Lifecycle.AFTER_START_EVENT.equals(event.getType())) {
      workerPool.submit(new EventsHandler(mbeanEventsQueue));
    } else if (Lifecycle.AFTER_STOP_EVENT.equals(event.getType())) {
      try {
        if (DEBUG) {
          log.debug("Closing RabbitMQ channel...");
        }
        channel.close();
      } catch (IOException e) {
      }
      try {
        if (DEBUG) {
          log.debug("Closing RabbitMQ connection...");
        }
        connection.close();
      } catch (IOException e) {
      }
    }

  }

  protected class EventsHandler implements Callable<EventsHandler> {
    protected String queue;
    protected QueueingConsumer consumer;
    protected Channel channel;
    protected boolean active = true;

    public EventsHandler(String queue) {
      this.queue = queue;
      try {
        this.channel = connection.createChannel();
        consumer = new QueueingConsumer(channel, incoming);
        if (DEBUG) {
          log.debug("Consuming events on q: " + queue);
        }
        channel.basicConsume(queue, true, consumer);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

    public boolean isActive() {
      return active;
    }

    public synchronized void setActive(boolean active) {
      this.active = active;
    }

    public EventsHandler call() throws Exception {
      while (active) {
        if (DEBUG) {
          log.debug("Waiting for delivery...");
        }
        QueueingConsumer.Delivery delivery = incoming.take();
        if (DEBUG) {
          log.debug("Processing delivery: " + delivery.toString());
        }

        CloudMBeanInvoker invoker = new StandardMBeanInvoker();
        invoker.setReplyTo(delivery.getProperties().getReplyTo());
        invoker.setCorrelationId(delivery.getProperties().getCorrelationId());
        ObjectMapper omapper = new ObjectMapper();
        Map<String, Object> request = omapper.readValue(new ByteArrayInputStream(delivery.getBody()), Map.class);
        if (DEBUG) {
          log.debug("Request: " + request.toString());
        }
        if (request.containsKey("mbean")) {
          invoker.setName(request.get("mbean").toString());
        }
        if (request.containsKey("attribute")) {
          invoker.setAttributeName(request.get("attribute").toString());
        }
        if (request.containsKey("operation")) {
          invoker.setOperation(request.get("operation").toString());
          if (request.containsKey("parameters")) {
            Object o = request.get("parameters");
            if (DEBUG) {
              log.debug("params: " + String.valueOf(o));
            }
            if (null != o && o instanceof List) {
              List<List<Object>> params = (List<List<Object>>) o;
              String[] argTypes = new String[params.size()];
              Object[] args = new Object[params.size()];
              for (int i = 0; i < params.size(); i++) {
                List<Object> param = params.get(i);
                argTypes[i] = param.get(0).toString();
                args[i] = param.get(1);
              }
              invoker.setArgs(args);
              invoker.setArgTypes(argTypes);
            }
          }
        }

        workerPool.submit(invoker);
      }
      return this;
    }
  }

  protected class StandardMBeanInvoker implements CloudMBeanInvoker {

    protected String name;
    protected String operation = null;
    protected String attributeName = null;
    protected Object[] args = new Object[0];
    protected String[] argTypes = new String[0];
    protected ObjectName oname;
    protected Channel channel;
    protected String correlationId;
    protected String replyTo;

    public StandardMBeanInvoker() {
      try {
        this.channel = connection.createChannel();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

    public void setName(String name) throws MalformedObjectNameException {
      this.name = name;
      this.oname = new ObjectName(name);
    }

    public String getName() {
      return this.name;
    }

    public ObjectName getObjectName() {
      return oname;
    }

    public void setOperation(String name) {
      this.operation = name;
    }

    public String getOperation() {
      return this.operation;
    }

    public void setAttributeName(String name) {
      this.attributeName = name;
    }

    public String getAttributeName() {
      return this.attributeName;
    }

    public void setCorrelationId(String id) {
      this.correlationId = id;
    }

    public String getCorrelationId() {
      return this.correlationId;
    }

    public void setReplyTo(String replyTo) {
      this.replyTo = replyTo;
    }

    public String getReplyTo() {
      return this.replyTo;
    }

    public void setArgs(Object[] args) {
      this.args = args;
    }

    public Object[] getArgs() {
      return this.args;
    }

    public void setArgTypes(String[] argTypes) {
      this.argTypes = argTypes;
    }

    public String[] getArgTypes() {
      return this.argTypes;
    }

    public Object getAttributeValue(String attributeName) {
      Object o = null;
      try {
        o = mbeanServer.getAttribute(oname, attributeName);
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
      return o;
    }

    public Object invoke() {
      Object o = null;
      try {
        o = mbeanServer.invoke(oname, operation, args, argTypes);
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
      return o;
    }

    public Object call() throws Exception {
      AMQP.BasicProperties props = new AMQP.BasicProperties();
      props.setCorrelationId(correlationId);
      Object o = null;
      if (null != operation) {
        o = invoke();
      } else if (null != attributeName) {
        o = mbeanServer.getAttribute(oname, attributeName);
      }
      if (null != o) {
        if (DEBUG) {
          log.debug("Invocation returned: " + o.toString());
          log.debug("Class: " + o.getClass().toString());
        }
      }
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      JsonGenerator json = new JsonFactory().createJsonGenerator(bytesOut, JsonEncoding.UTF8);
      json.writeStartObject();
      // We're getting runtime JVM information
      if (o instanceof CompositeDataSupport) {
        CompositeDataSupport data = (CompositeDataSupport) o;
        for (String key : data.getCompositeType().keySet()) {
          if (DEBUG) {
            log.debug(key + ": " + data.get(key));
          }
          Object item = data.get(key);
          if (item instanceof Long) {
            json.writeNumberField(key, (Long) item);
          } else if (item instanceof String) {
            json.writeStringField(key, (String) item);
          } else {
            json.writeStringField(key, item.toString());
          }
        }
      } else if (o instanceof Long) {
        json.writeNumberField(attributeName, (Long) o);
      } else if (o instanceof String) {
        json.writeStringField(attributeName, (String) o);
      } else {
        if (DEBUG) {
          log.debug("Not sure what to do with " + attributeName);
        }
      }
      json.writeEndObject();
      json.flush();

      channel.basicPublish("", getReplyTo(), props, bytesOut.toByteArray());

      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }

}
