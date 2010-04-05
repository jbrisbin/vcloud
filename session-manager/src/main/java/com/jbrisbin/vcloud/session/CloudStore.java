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

import com.rabbitmq.client.*;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.session.StoreBase;
import org.apache.catalina.util.Base64;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 2, 2010 Time: 5:20:28 PM To change this template use File |
 * Settings | File Templates.
 */
public class CloudStore extends StoreBase {

  static final String info = "CloudStore/1.0";
  static final String name = "CloudStore";

  // RabbitMQ
  protected String mqHost = "localhost";
  protected int mqPort = 5672;
  protected String mqUser = "guest";
  protected String mqPassword = "guest";
  protected String mqVirtualHost = "/";
  protected int maxMqHandlers = 2;
  protected String eventsExchange = "amq.fanout";
  protected String replicationEventsExchange = "amq.topic";
  protected String replicationEventsQueue = null;
  protected String eventsQueue = null;
  protected String sourceEventsExchange = "amq.topic";
  protected String sourceEventsQueue = null;
  protected Connection mqConnection;
  protected long loadTimeout = 15000L;

  // Unique store key to identify this node
  protected String storeKey;

  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  protected BlockingQueue<CloudSessionMessage> updateEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected BlockingQueue<CloudSessionMessage> loadEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected BlockingQueue<CloudSessionMessage> replicationEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected Timer timer = new Timer();

  // Local/private sessions
  protected ConcurrentHashMap<String, String> cloudSessions = new ConcurrentHashMap<String, String>();
  protected ConcurrentHashMap<String, CloudSession> localSessions = new ConcurrentHashMap<String, CloudSession>();
  protected ConcurrentHashMap<String, SessionLoader> sessionLoaders = new ConcurrentHashMap<String, SessionLoader>();
  protected ConcurrentHashMap<String, Future<Session>> sessionLoaderFutures = new ConcurrentHashMap<String, Future<Session>>();

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

  public int getMaxMqHandlers() {
    return maxMqHandlers;
  }

  public void setMaxMqHandlers(int maxMqHandlers) {
    this.maxMqHandlers = maxMqHandlers;
  }

  public String getEventsExchange() {
    return eventsExchange;
  }

  public void setEventsExchange(String eventsExchange) {
    this.eventsExchange = eventsExchange;
  }

  public ConcurrentHashMap<String, CloudSession> getLocalSessions() {
    return localSessions;
  }

  public String getEventsQueue() {
    return eventsQueue;
  }

  public void setEventsQueue(String eventsQueue) {
    this.eventsQueue = eventsQueue;
  }

  public String getReplicationEventsExchange() {
    return replicationEventsExchange;
  }

  public void setReplicationEventsExchange(String replicationEventsExchange) {
    this.replicationEventsExchange = replicationEventsExchange;
  }

  public String getReplicationEventsQueue() {
    return replicationEventsQueue;
  }

  public void setReplicationEventsQueue(String replicationEventsQueue) {
    this.replicationEventsQueue = replicationEventsQueue;
  }

  public String getSourceEventsExchange() {
    return sourceEventsExchange;
  }

  public void setSourceEventsExchange(String sourceEventsExchange) {
    this.sourceEventsExchange = sourceEventsExchange;
  }

  public String getSourceEventsQueue() {
    return sourceEventsQueue;
  }

  public void setSourceEventsQueue(String sourceEventsQueue) {
    this.sourceEventsQueue = sourceEventsQueue;
  }

  public String getStoreKey() {
    return storeKey;
  }

  public void setStoreKey(String storeKey) {
    this.storeKey = storeKey;
  }

  /**
   * Get session loader timeout.
   *
   * @return
   */
  public int getLoadTimeout() {
    return (int) (loadTimeout / 1000);
  }

  /**
   * Set session loader timeout (in seconds).
   *
   * @param loadTimeout
   */
  public void setLoadTimeout(int loadTimeout) {
    this.loadTimeout = (long) (loadTimeout * 1000);
  }

  @Override
  public String getInfo() {
    return info;
  }

  @Override
  public String getStoreName() {
    return name;
  }

  public int getSize() throws IOException {
    return cloudSessions.size();
  }

  public String[] keys() throws IOException {
    return cloudSessions.keySet().toArray(new String[getSize()]);
  }

  public Session load(String id) throws ClassNotFoundException, IOException {
    try {
      return workerPool.submit(new SessionLoader(id)).get();
    } catch (InterruptedException e) {
      manager.getContainer().getLogger().error(e.getMessage(), e);
    } catch (ExecutionException e) {
      manager.getContainer().getLogger().error(e.getMessage(), e);
    }
    return null;
  }

  public void remove(String id) throws IOException {
    sendEvent("destroy", id.getBytes());
  }

  public void clear() throws IOException {
    sendEvent("clear", new byte[0]);
  }

  public void save(Session session) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void start() throws LifecycleException {
    super.start();
    storeKey = generateStoreKey();

    ConnectionParameters cparams = new ConnectionParameters();
    cparams.setUsername(mqUser);
    cparams.setPassword(mqPassword);
    cparams.setVirtualHost(mqVirtualHost);
    Channel mqChannel = null;
    try {
      mqConnection = new ConnectionFactory(cparams).newConnection(mqHost, mqPort);
      mqChannel = mqConnection.createChannel();

      // Messages bound for all nodes in cluster go here
      mqChannel.exchangeDeclare(eventsExchange, "fanout", true);
      mqChannel.queueDeclare(eventsQueue, true);
      mqChannel.queueBind(eventsQueue, eventsExchange, "");

      // Messages bound for just this node go here
      mqChannel.exchangeDeclare(sourceEventsExchange, "direct", true);
      mqChannel.queueDeclare(sourceEventsQueue, true);
      String sourceEventsRoutingKey = "session.events." + storeKey;
      mqChannel.queueBind(sourceEventsQueue, sourceEventsExchange, sourceEventsRoutingKey);

      // Replication events
      mqChannel.exchangeDeclare(replicationEventsExchange, "topic", true);
      mqChannel.queueDeclare(replicationEventsQueue, true);
      mqChannel.queueBind(replicationEventsQueue, replicationEventsExchange, "replicate");

      // Start several handlers to keep throughput high
      for (int i = 0; i < maxMqHandlers; i++) {
        workerPool.submit(new SessionEventListener(eventsQueue));
        workerPool.submit(new UpdateEventHandler());
        workerPool.submit(new LoadEventHandler());
      }
      // Only use one replication event handler
      workerPool.submit(new ReplicationEventHandler());

      // Keep the session loader pool clear of dead loaders
      timer.scheduleAtFixedRate(new SessionLoaderScavenger(), loadTimeout, loadTimeout);

    } catch (IOException e) {
      manager.getContainer().getLogger().error(e.getMessage(), e);
    } finally {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  @Override
  public void stop() throws LifecycleException {
    try {
      this.mqConnection.close();
    } catch (IOException e) {
      manager.getContainer().getLogger().error(e.getMessage(), e);
    }
  }

  protected void sendEvent(String type, byte[] body) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("type", type);
    headers.put("source", storeKey);
    props.setHeaders(headers);
    Channel mqChannel = mqConnection.createChannel();
    mqChannel.basicPublish(eventsExchange, "", props, body);
    mqChannel.close();
  }

  protected String generateStoreKey() {
    byte[] b = new byte[16];
    new Random().nextBytes(b);
    return new String(Base64.encode(b));
  }

  protected void serializeSessionToMessage(CloudSessionMessage msg) throws IOException {
    if (localSessions.containsKey(msg.getId())) {
      CloudSession session = localSessions.get(msg.getId());
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
      session.writeObjectData(objectOut);
      objectOut.flush();
      objectOut.close();
      bytesOut.flush();
      bytesOut.close();
      localSessions.remove(msg.getId());
      byte[] buff = bytesOut.toByteArray();
      msg.setBody(buff);
    } else {
      msg.setBody(new byte[0]);
    }
  }

  protected CloudSession deserializeSessionFromMessage(
      CloudSessionMessage msg) throws IOException, ClassNotFoundException {
    CloudSession session = (CloudSession) manager.createEmptySession();
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(msg.getBody());
    ObjectInputStream objectIn = new ObjectInputStream(bytesIn);
    session.readObjectData(objectIn);
    session.setManager(manager);
    localSessions.put(msg.getId(), session);
    return session;
  }

  protected class SessionEventListener implements Callable<SessionEventListener> {

    protected Channel mqChannel = null;
    protected QueueingConsumer eventsConsumer;

    protected SessionEventListener(String queue) throws IOException {
      mqChannel = mqConnection.createChannel();
      eventsConsumer = new QueueingConsumer(mqChannel);
      mqChannel.basicConsume(queue, eventsConsumer);
    }

    public SessionEventListener call() throws Exception {
      // Loop forever, processing incoming events
      QueueingConsumer.Delivery delivery;
      while (null != (delivery = eventsConsumer.nextDelivery())) {
        Map<String, Object> headers = delivery.getProperties().getHeaders();
        if (headers.containsKey("source")) {
          String source = headers.get("source").toString();
          if (!source.equals(storeKey)) {
            if (headers.containsKey("type")) {
              String id;
              CloudSessionMessage msg;
              switch (CloudSession.Events.valueOf(headers.get("type").toString().toUpperCase())) {
                case CREATE:
                  id = new String(delivery.getBody());
                  cloudSessions.put(id, source);
                  break;
                case DESTROY:
                  id = new String(delivery.getBody());
                  cloudSessions.remove(id);
                  break;
                case LOAD:
                  id = new String(delivery.getBody());
                  msg = new CloudSessionMessage();
                  msg.setType("load");
                  msg.setSource(source);
                  msg.setId(id);
                  loadEvents.put(msg);
                  break;
                case UPDATE:
                  msg = new CloudSessionMessage();
                  msg.setType("update");
                  msg.setId(headers.get("id").toString());
                  msg.setBody(delivery.getBody());
                  updateEvents.add(msg);
                  break;
                case CLEAR:
                  cloudSessions.clear();
                  break;
                case REPLICATE:
                  id = new String(delivery.getBody());
                  msg = new CloudSessionMessage();
                  msg.setType("replicate");
                  msg.setSource(source);
                  msg.setId(id);
                  replicationEvents.put(msg);
                  break;
              }
            }
            mqChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          }
        }
      }
      return this;
    }
  }

  protected class ReplicationEventHandler implements Callable<ReplicationEventHandler> {

    protected Channel mqChannel = null;

    public ReplicationEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    public ReplicationEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while (null != (msg = replicationEvents.take())) {
        CloudSession session = deserializeSessionFromMessage(msg);
        localSessions.put(msg.getId(), session);
      }
      return this;
    }
  }

  protected class UpdateEventHandler implements Callable<UpdateEventHandler> {

    protected Channel mqChannel = null;

    public UpdateEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    public UpdateEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while (null != (msg = updateEvents.take())) {
        CloudSession session = deserializeSessionFromMessage(msg);
        SessionLoader loader = sessionLoaders.remove(session.getId());
        loader.getResponseQueue().add(session);
      }
      return this;
    }
  }

  protected class LoadEventHandler implements Callable<LoadEventHandler> {

    protected Channel mqChannel = null;

    public LoadEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    public LoadEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while (null != (msg = loadEvents.take())) {
        serializeSessionToMessage(msg);

        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props.setContentType("application/octet-stream");
        Map<String, Object> headers = new LinkedHashMap<String, Object>();
        headers.put("type", "update");
        headers.put("source", storeKey);
        headers.put("id", msg.getId());

        mqChannel.basicPublish(sourceEventsExchange, "session.events." + msg.getSource(), props, msg.getBody());
      }
      return this;
    }
  }

  protected class SessionLoader implements Callable<CloudSession> {

    protected String id;
    protected Channel mqChannel = null;
    protected BlockingQueue<CloudSession> responseQueue = new LinkedBlockingQueue<CloudSession>();
    protected long startTime;

    protected SessionLoader(String id) throws IOException {
      this.id = id;
      mqChannel = mqConnection.createChannel();
      startTime = System.currentTimeMillis();
    }

    public String getId() {
      return id;
    }

    public long getStartTime() {
      return startTime;
    }

    public BlockingQueue<CloudSession> getResponseQueue() {
      return responseQueue;
    }

    public CloudSession call() throws Exception {
      if (cloudSessions.containsKey(id)) {
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        Map<String, Object> headers = new LinkedHashMap<String, Object>();
        headers.put("type", "load");
        headers.put("source", storeKey);

        mqChannel.basicPublish(sourceEventsExchange,
            "session.events." + cloudSessions.get(id).toString(),
            props,
            id.getBytes());
        sessionLoaders.put(id, this);
        CloudSession session = responseQueue.take();
        close();

        return session;
      } else {
        return null;
      }
    }

    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  protected class SessionLoaderScavenger extends TimerTask {
    public void run() {
      for (SessionLoader loader : sessionLoaders.values()) {
        long now = System.currentTimeMillis();
        if ((now - loader.getStartTime()) > loadTimeout) {
          sessionLoaders.remove(loader);
          loader.close();
          Future<Session> future = sessionLoaderFutures.remove(loader.getId());
          if (!future.isDone()) {
            future.cancel(true);
          }
        }
      }
    }
  }
}
