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
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.modeler.Registry;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 2, 2010 Time: 5:20:28 PM To change this template use File |
 * Settings | File Templates.
 */
public class CloudStore extends StoreBase {

  static final String info = "CloudStore/1.0";
  static final String name = "CloudStore";

  protected Log log = LogFactory.getLog("vcloud");
  protected ObjectName objectName;

  // RabbitMQ
  protected String mqHost = "localhost";
  protected int mqPort = 5672;
  protected String mqUser = "guest";
  protected String mqPassword = "guest";
  protected String mqVirtualHost = "/";
  protected int maxMqHandlers = 2;
  protected String eventsExchange = "amq.fanout";
  protected String eventsQueue = null;
  protected String replicationEventsExchange = "amq.topic";
  protected String replicationEventsQueue = null;
  protected String sourceEventsExchange = "amq.topic";
  protected String sourceEventsQueue = null;
  protected Connection mqConnection;
  protected long loadTimeout = 15000L;

  // Unique store key to identify this node
  protected String storeId;

  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  protected BlockingQueue<CloudSessionMessage> updateEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected BlockingQueue<CloudSessionMessage> loadEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected BlockingQueue<CloudSessionMessage> replicationEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected Timer timer = new Timer();

  // Local/private sessions
  protected ConcurrentHashMap<String, String> cloudSessions = new ConcurrentHashMap<String, String>();
  protected ConcurrentHashMap<String, CloudSession> localSessions = new ConcurrentHashMap<String, CloudSession>();
  // Get session IDs
  protected ConcurrentHashMap<String, SessionLoader> getIdsLoaders = new ConcurrentHashMap<String, SessionLoader>();
  protected ConcurrentHashMap<String, Future<Session>> getIdsLoaderFutures = new ConcurrentHashMap<String, Future<Session>>();
  // Get session data
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

  public String getStoreId() {
    return storeId;
  }

  public void setStoreId(String storeId) {
    this.storeId = storeId;
  }

  public int getUpdateEventsCount() {
    return updateEvents.size();
  }

  public int getLoadEventsCount() {
    return sessionLoaderFutures.size();
  }

  public int getReplicationEventsCount() {
    return replicationEvents.size();
  }

  public String[] getCloudSessionIds() {
    return cloudSessions.keySet().toArray(new String[cloudSessions.size()]);
  }

  public String[] getLocalSessionIds() {
    return localSessions.keySet().toArray(new String[localSessions.size()]);
  }

  public Map<String, String> getCloudSessionMap() {
    return cloudSessions;
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
      log.error(e.getMessage(), e);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  public void remove(String id) throws IOException {
    sendEvent("destroy", id.getBytes());
  }

  public void clear() throws IOException {
    sendEvent("clear", new byte[0]);
  }

  public void clearLocalSessions() {
    localSessions.clear();
  }

  public void save(Session session) throws IOException {
    if (!localSessions.containsKey(session.getId())) {
      localSessions.put(session.getId(), (CloudSession) session);
      sendEvent("touch", session.getId().getBytes());
      // Replicate this session elsewhere
      AMQP.BasicProperties props = new AMQP.BasicProperties();
      Map<String, Object> headers = new LinkedHashMap<String, Object>();
      headers.put("type", "replicate");
      headers.put("source", storeId);

      Channel mqChannel = mqConnection.createChannel();
      SessionSerializer serializer = new InternalSessionSerializer();
      serializer.setSession(session);
      mqChannel.basicPublish(sourceEventsExchange,
          "session.replication",
          props,
          serializer.serialize());
      mqChannel.close();
    }
  }

  @Override
  public void start() throws LifecycleException {
    super.start();
    if (log.isDebugEnabled()) {
      log.debug("Starting CloudStore: " + storeId);
    }

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
      String sourceEventsRoutingKey = "session.requests." + storeId;
      mqChannel.queueBind(sourceEventsQueue, sourceEventsExchange, sourceEventsRoutingKey);

      // Replication events
      mqChannel.exchangeDeclare(replicationEventsExchange, "topic", true);
      mqChannel.queueDeclare(replicationEventsQueue, true);
      mqChannel.queueBind(replicationEventsQueue, replicationEventsExchange, "session.replication");

      // Start several handlers to keep throughput high
      workerPool.submit(new SessionEventListener(eventsQueue));
      for (int i = 0; i < maxMqHandlers; i++) {
        workerPool.submit(new SessionEventListener(sourceEventsQueue));
        workerPool.submit(new SessionEventListener(replicationEventsQueue));
        workerPool.submit(new UpdateEventHandler());
        workerPool.submit(new LoadEventHandler());
      }
      // Only use one replication event handler
      workerPool.submit(new ReplicationEventHandler());

      // Keep the session loader pool clear of dead loaders
      timer.scheduleAtFixedRate(new SessionLoaderScavenger(), loadTimeout, loadTimeout);

    } catch (IOException e) {
      log.error(e.getMessage(), e);
    } finally {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }

    try {
      sendEvent("getids", new byte[0]);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    try {
      objectName = new ObjectName("vCloud:type=SessionStore,id=" + storeId);
      Registry.getRegistry(null, null).registerComponent(this, objectName, null);
    } catch (MalformedObjectNameException e) {
      log.error(e.getMessage(), e);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }

  }

  @Override
  public void stop() throws LifecycleException {
    try {
      this.mqConnection.close();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    Registry.getRegistry(null, null).unregisterComponent(objectName);
  }

  protected byte[] serializeLocalSessions() {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
      for (Map.Entry<String, CloudSession> entry : localSessions.entrySet()) {
        entry.getValue().writeObjectData(objectOut);
      }
      objectOut.flush();
      objectOut.close();

      bytesOut.flush();
      bytesOut.close();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return bytesOut.toByteArray();
  }

  protected void sendEvent(String type, byte[] body) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("type", type);
    headers.put("source", storeId);
    props.setHeaders(headers);
    Channel mqChannel = mqConnection.createChannel();
    mqChannel.basicPublish(eventsExchange, "", props, body);
    mqChannel.close();
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
          if (headers.containsKey("type")) {
            String id;
            CloudSessionMessage msg;
            switch (CloudSession.Events.valueOf(headers.get("type").toString().toUpperCase())) {
              case TOUCH:
                id = new String(delivery.getBody());
                cloudSessions.put(id, source);
                break;
              case DESTROY:
                id = new String(delivery.getBody());
                cloudSessions.remove(id);
                break;
              case LOAD:
                id = new String(delivery.getBody());
                if (log.isDebugEnabled()) {
                  log.debug("Received load request for " + id + " from " + source);
                }
                msg = new CloudSessionMessage();
                msg.setType("load");
                msg.setSource(source);
                msg.setId(id);
                loadEvents.put(msg);
                break;
              case UPDATE:
                if (log.isDebugEnabled()) {
                  log.debug("Received update event from " + source);
                }
                msg = new CloudSessionMessage();
                msg.setType("update");
                msg.setId(headers.get("id").toString());
                msg.setBody(delivery.getBody());
                updateEvents.add(msg);
                break;
              case REPLICATE:
                msg = new CloudSessionMessage();
                msg.setType("replicate");
                msg.setSource(source);
                msg.setId(headers.get("id").toString());
                msg.setBody(delivery.getBody());
                replicationEvents.put(msg);
                break;
              case CLEAR:
                cloudSessions.clear();
                break;
              case GETALL:
                workerPool.submit(new GetAllEventHandler(source));
                break;
              case GETIDS:
                workerPool.submit(new GetIdsEventHandler());
                break;
            }
          }
          mqChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
      }
      return this;
    }
  }

  protected class ReplicationEventHandler implements Callable<ReplicationEventHandler> {
    public ReplicationEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while (null != (msg = replicationEvents.take())) {
        CloudSession session = (CloudSession) manager.createEmptySession();
        SessionDeserializer deserializer = new InternalSessionDeserializer();
        deserializer.setSession(session);
        deserializer.setBytes(msg.getBody());
        localSessions.put(session.getId(), session);
        cloudSessions.put(session.getId(), msg.getSource());
        if (log.isDebugEnabled()) {
          log.debug("Replicated session " + msg.getId());
        }
      }
      return this;
    }
  }

  protected class UpdateEventHandler implements Callable<UpdateEventHandler> {
    public UpdateEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while (null != (msg = updateEvents.take())) {
        CloudSession session = (CloudSession) manager.createEmptySession();
        SessionDeserializer deserializer = new InternalSessionDeserializer();
        deserializer.setSession(session);
        deserializer.setBytes(msg.getBody());
        if (sessionLoaders.containsKey(session.getId())) {
          SessionLoader loader = sessionLoaders.remove(session.getId());
          loader.getResponseQueue().add(session);
        }
        localSessions.put(session.getId(), session);
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
        if (localSessions.containsKey(msg.getId())) {
          CloudSession session = localSessions.remove(msg.getId());
          SessionSerializer serializer = new InternalSessionSerializer();
          serializer.setSession(session);
          msg.setBody(serializer.serialize());

          AMQP.BasicProperties props = new AMQP.BasicProperties();
          props.setContentType("application/octet-stream");
          Map<String, Object> headers = new LinkedHashMap<String, Object>();
          headers.put("type", "update");
          headers.put("source", storeId);
          headers.put("id", msg.getId());

          mqChannel.basicPublish(sourceEventsExchange, "session.requests." + msg.getSource(), props, msg.getBody());
        }
      }
      return this;
    }

    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  protected class GetAllEventHandler implements Callable<GetAllEventHandler> {

    protected Channel mqChannel = null;
    protected String source;

    public GetAllEventHandler(String source) throws IOException {
      mqChannel = mqConnection.createChannel();
      this.source = source;
    }

    public GetAllEventHandler call() throws Exception {
      for (Map.Entry<String, CloudSession> entry : localSessions.entrySet()) {
        CloudSessionMessage msg = new CloudSessionMessage();
        msg.setType("load");
        msg.setSource(source);
        msg.setId(entry.getKey());
        loadEvents.put(msg);
      }
      close();
      return this;
    }

    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  protected class GetIdsEventHandler implements Callable<GetIdsEventHandler> {

    protected Channel mqChannel = null;

    public GetIdsEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    public GetIdsEventHandler call() throws Exception {
      for (Map.Entry<String, CloudSession> entry : localSessions.entrySet()) {
        sendEvent("touch", entry.getKey().getBytes());
      }
      close();
      return this;
    }

    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
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
        headers.put("source", storeId);

        mqChannel.basicPublish(sourceEventsExchange,
            "session.requests." + cloudSessions.get(id),
            props,
            id.getBytes());
        sessionLoaders.put(id, this);
        CloudSession session = responseQueue.take();
        close();
        localSessions.put(id, session);

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
          try {
            sessionLoaders.remove(loader);
            Future<Session> future = sessionLoaderFutures.remove(loader.getId());
            if (null != future && !future.isDone()) {
              future.cancel(true);
            }
          } catch (Throwable t) {
            // IGNORED
          }
        }
      }
    }
  }
}
