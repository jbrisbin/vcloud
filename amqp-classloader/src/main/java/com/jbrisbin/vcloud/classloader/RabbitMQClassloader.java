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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.concurrent.*;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RabbitMQClassLoader extends ClassLoader {

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
  protected int loadTimeout = 3;

  protected ConnectionFactory factory = new ConnectionFactory();
  protected Connection connection;
  protected Channel channel;
  protected ExecutorService workers = Executors.newCachedThreadPool();
  protected ConcurrentSkipListMap<String, Class<?>> classCache = new ConcurrentSkipListMap<String, Class<?>>();

  public RabbitMQClassLoader(ClassLoader parent) {
    super(parent);
    setFactoryDefaults();
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

  public int getLoadTimeout() {
    return loadTimeout;
  }

  public void setLoadTimeout(int loadTimeout) {
    this.loadTimeout = loadTimeout;
  }

  protected void declareResources(Channel mq) throws IOException {
    synchronized (mq) {
      mq.exchangeDeclare(exchange, "topic", true, false, null);
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
      declareResources(channel);
    }
    return channel;
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    return findClass(name);
  }

  @Override
  protected URL findResource(String name) {
    return super.findResource(name);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  protected Package getPackage(String name) {
    return super.getPackage(name);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return loadClass(name, true);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    Class<?> clazz = null;
    try {
      clazz = super.findClass(name);
      if (debug) {
        log.debug("Found class locally defined: " + name);
      }
      return clazz;
    } catch (ClassNotFoundException ignored) {
      clazz = classCache.get(name);
      if (null == clazz) {
        // Class doesn't exist locally, yet
        if (debug) {
          log.debug("Class not yet cached, loading remotely");
        }
        RemoteLoader loader = null;
        Future<?> f = null;
        try {
          loader = new RemoteLoader("class", name);
          f = workers.submit(loader);
          loader.getCountdown().await();
          // It should be in the cache now
          clazz = classCache.get(name);
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        } finally {
          if (null != f) {
            if (!f.isDone()) {
              f.cancel(true);
            }
          }
        }
      }

      if (debug) {
        log.debug("Returning class data: " + clazz);
      }
      return clazz;
    }
  }

  class RemoteLoader implements Runnable {

    Channel mq;
    String type;
    String name;
    String queue;
    BlockingQueue<QueueingConsumer.Delivery> q = new LinkedBlockingQueue<QueueingConsumer.Delivery>();
    CountDownLatch countdown = new CountDownLatch(1);

    RemoteLoader(String type, String name) throws IOException {
      this.type = type;
      this.name = name;
      mq = getConnection().createChannel();
      if (debug) {
        log.debug("Remotely loading " + type + "://" + name);
      }
      queue = mq.queueDeclare().getQueue();
    }

    public CountDownLatch getCountdown() {
      return countdown;
    }

    @Override
    public void run() {
      String tag = String.format("%s-%s", System.currentTimeMillis(), name);
      try {
        QueueingConsumer consumer = new QueueingConsumer(mq, q);
        mq.basicConsume(queue, true, tag, consumer);

        AMQP.BasicProperties properties = new AMQP.BasicProperties();
        properties.setReplyTo(queue);
        properties.setType(type);

        mq.basicPublish(exchange, name, properties, new byte[0]);

        byte[] bytes;
        QueueingConsumer.Delivery delivery = q.poll(loadTimeout, TimeUnit.SECONDS);
        bytes = delivery.getBody();
        if (null != bytes && bytes.length > 0) {
          ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytes));
          Class<?> clazz = (Class<?>) oin.readObject();
          if (debug) {
            log.debug("Received class " + clazz + " from remote server");
          }
          classCache.put(name, clazz);
          countdown.countDown();
        }
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      } finally {
        try {
          mq.basicCancel(tag);
          mq.close();
        } catch (IOException e) {
          if (debug) {
            log.debug(e.getMessage(), e);
          }
        }
      }
    }

  }

}
