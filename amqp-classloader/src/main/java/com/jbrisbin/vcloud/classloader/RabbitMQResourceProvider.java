/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.jbrisbin.vcloud.classloader;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RabbitMQResourceProvider {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected boolean debug = log.isDebugEnabled();

  protected String mqHost = "localhost";
  protected int mqPort = 5672;
  protected String mqUser = "guest";
  protected String mqPassword = "guest";
  protected String mqVirtualHost = "/";
  protected int mqRequestedHeartbeat = 15;
  protected int maxListeners = 3;
  protected String exchange = "vcloud.classloaders";
  protected String queue = null;
  protected String routingKey = "#";
  protected AtomicBoolean active = new AtomicBoolean(true);

  protected ClassLoader parent;

  protected ConnectionFactory factory = new ConnectionFactory();
  protected Connection connection;
  protected Channel channel;
  protected ExecutorService listeners = Executors.newCachedThreadPool();
  protected List<Future> activeListeners = new ArrayList<Future>();
  protected LinkedBlockingQueue<OutgoingMessage> outgoing = new LinkedBlockingQueue<OutgoingMessage>();

  public RabbitMQResourceProvider(ClassLoader parent) {
    this.parent = parent;
    setFactoryDefaults();
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public String getMqHost() {
    return mqHost;
  }

  public void setMqHost(String mqHost) {
    this.mqHost = mqHost;
    factory.setHost(mqHost);
  }

  public int getMqPort() {
    return mqPort;
  }

  public void setMqPort(int mqPort) {
    this.mqPort = mqPort;
    factory.setPort(mqPort);
  }

  public String getMqUser() {
    return mqUser;
  }

  public void setMqUser(String mqUser) {
    this.mqUser = mqUser;
    factory.setUsername(mqUser);
  }

  public String getMqPassword() {
    return mqPassword;
  }

  public void setMqPassword(String mqPassword) {
    this.mqPassword = mqPassword;
    factory.setPassword(mqPassword);
  }

  public String getMqVirtualHost() {
    return mqVirtualHost;
  }

  public void setMqVirtualHost(String mqVirtualHost) {
    this.mqVirtualHost = mqVirtualHost;
    factory.setVirtualHost(mqVirtualHost);
  }

  public int getMqRequestedHeartbeat() {
    return mqRequestedHeartbeat;
  }

  public void setMqRequestedHeartbeat(int mqRequestedHeartbeat) {
    this.mqRequestedHeartbeat = mqRequestedHeartbeat;
    factory.setRequestedHeartbeat(mqRequestedHeartbeat);
  }

  public int getMaxListeners() {
    return maxListeners;
  }

  public void setMaxListeners(int maxListeners) {
    this.maxListeners = maxListeners;
  }

  public String getExchange() {
    return exchange;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public void setRoutingKey(String routingKey) {
    this.routingKey = routingKey;
  }

  public boolean isActive() {
    return active.get();
  }

  public void setActive(boolean active) {
    this.active.set(active);
  }

  public void start() throws IOException {
    try {
      declareResources();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    if (debug) {
      log.debug("Starting " + maxListeners + " message handlers and " + maxListeners + " message senders.");
    }
    for (int i = 0; i < maxListeners; i++) {
      activeListeners.add(listeners.submit(new ClassLoadHandler()));
      activeListeners.add(listeners.submit(new ClassSendHandler()));
    }
  }

  public void stop() {
    for (Future f : activeListeners) {
      if (!f.isDone()) {
        f.cancel(true);
      }
    }
    listeners.shutdownNow();
  }

  protected void declareResources() throws IOException {
    Channel mq = getChannel();
    synchronized (mq) {
      mq.exchangeDeclare(exchange, "topic", true, false, null);
      if (null == this.queue) {
        queue = mq.queueDeclare().getQueue();
      } else {
        mq.queueDeclare(this.queue, true, false, true, null);
      }
      mq.queueBind(queue, exchange, routingKey);
    }
  }

  protected void setFactoryDefaults() {
    factory.setHost(mqHost);
    factory.setPort(mqPort);
    factory.setUsername(mqUser);
    factory.setPassword(mqPassword);
    factory.setVirtualHost(mqVirtualHost);
    factory.setRequestedHeartbeat(mqRequestedHeartbeat);
  }

  protected Connection getConnection() throws IOException {
    if (null == connection) {
      connection = factory.newConnection();
    }
    return connection;
  }

  protected Channel getChannel() throws IOException {
    if (null == channel) {
      channel = getConnection().createChannel();
    }
    return channel;
  }

  class ClassLoadHandler implements Callable<ClassLoadHandler> {

    Channel mq;
    QueueingConsumer consumer;

    ClassLoadHandler() throws IOException {
      mq = getConnection().createChannel();
      consumer = new QueueingConsumer(mq);
      mq.basicConsume(queue, true, consumer);
    }

    @Override
    public ClassLoadHandler call() throws Exception {
      QueueingConsumer.Delivery delivery;
      while (active.get() && null != (delivery = consumer.nextDelivery())) {
        // This is going back to the requestor
        OutgoingMessage outgoingMessage = new OutgoingMessage();
        // What resource are we loading?
        String resourceName = new String(delivery.getEnvelope().getRoutingKey());
        outgoingMessage.setResourceName(resourceName);
        // Where do we send when loaded?
        String replyTo = delivery.getProperties().getReplyTo();
        outgoingMessage.setReplyTo(replyTo);

        // Is this a class or a resource?
        String type = delivery.getProperties().getType();
        if (debug) {
          log.debug("Loading " + type + "://" + resourceName + " for " + replyTo);
        }

        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        if ("class".equalsIgnoreCase(type)) {
          try {
            Class clazz = parent.loadClass(resourceName);
            ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
            objectOut.writeObject(clazz);
            objectOut.flush();
          } catch (ClassNotFoundException notFound) {
            log.error(notFound.getMessage(), notFound);
          }
        } else if ("resource".equalsIgnoreCase(type)) {
          InputStream in = parent.getResourceAsStream(resourceName);
          if (null != in) {
            byte[] buff = new byte[16384];
            int bytesRead = in.read(buff);
            for (; bytesRead > 0; bytesRead = in.read(buff)) {
              bytesOut.write(buff, 0, bytesRead);
            }
          }
        }
        bytesOut.flush();

        outgoingMessage.setBytes(bytesOut.toByteArray());
        outgoing.put(outgoingMessage);
      }
      return this;
    }

    public void close() {
      try {
        mq.close();
      } catch (IOException e) {
        // IGNORED
      }
    }

  }

  class ClassSendHandler implements Runnable {

    Channel mq;

    ClassSendHandler() throws IOException {
      mq = getConnection().createChannel();
    }

    @Override
    public void run() {
      OutgoingMessage message;
      try {
        while (active.get() && null != (message = outgoing.take())) {
          AMQP.BasicProperties properties = new AMQP.BasicProperties();
          properties.setType(message.getType());
          Map<String, Object> headers = new LinkedHashMap<String, Object>();
          headers.put("resourceName", message.getResourceName());
          properties.setHeaders(headers);

          try {
            mq.basicPublish("", message.getReplyTo(), properties, message.getBytes());
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
        }
      } catch (InterruptedException e) {
        // Thread probably cancelled us
      }
    }
  }


}
