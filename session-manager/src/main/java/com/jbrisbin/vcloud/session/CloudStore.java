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
import org.apache.catalina.Container;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.StoreBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.modeler.Registry;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA. User: jbrisbin Date: Apr 2, 2010 Time: 5:20:28 PM To change this template use File |
 * Settings | File Templates.
 */
public class CloudStore extends StoreBase {

  static final String info = "CloudStore/1.0";
  static final String name = "CloudStore";

  protected Log log = LogFactory.getLog(getClass());
  protected ObjectName objectName;
  protected String state = "stopped";

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
  protected String replicationEventsRoutingKey = "vcloud.session.replication";
  protected String sourceEventsExchange = "amq.topic";
  protected String sourceEventsQueue = null;
  protected String sourceEventsRoutingPrefix = "vcloud.source.";
  protected Connection mqConnection;
  protected long loadTimeout = 15;
  protected boolean deleteQueuesOnStop = true;

  // Unique store key to identify this node
  protected String storeId;

  protected DaemonThreadFactory daemonThreadFactory = new DaemonThreadFactory();
  protected ExecutorService listenerPool = Executors.newCachedThreadPool(daemonThreadFactory);
  protected ExecutorService workerPool = Executors.newCachedThreadPool(daemonThreadFactory);
  protected BlockingQueue<CloudSessionMessage> updateEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected BlockingQueue<CloudSessionMessage> loadEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  protected Timer timer = new Timer();

  // Local/private sessions
  protected Map<String, String> cloudSessions = new HashMap<String, String>();
  protected Map<String, CloudSession> localSessions = new HashMap<String, CloudSession>();

  // Get session data
  protected Map<String, BlockingQueue<CloudSession>> responseQueues = new HashMap<String, BlockingQueue<CloudSession>>();
  protected HashMap<String, SessionLoader> sessionLoaders = new HashMap<String, SessionLoader>();

  public CloudStore() {
  }

  public String getState() {
    return state;
  }

  public synchronized void setState(String state) {
    this.state = state;
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

  public Map<String, CloudSession> getLocalSessions() {
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

  public String getSourceEventsRoutingPrefix() {
    return sourceEventsRoutingPrefix;
  }

  public void setSourceEventsRoutingPrefix(String sourceEventsRoutingPrefix) {
    this.sourceEventsRoutingPrefix = sourceEventsRoutingPrefix;
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
    return sessionLoaders.size();
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
  public long getLoadTimeout() {
    return loadTimeout;
  }

  /**
   * Set session loader timeout (in seconds).
   *
   * @param loadTimeout
   */
  public void setLoadTimeout(long loadTimeout) {
    this.loadTimeout = loadTimeout;
  }

  public boolean isDeleteQueuesOnStop() {
    return deleteQueuesOnStop;
  }

  public void setDeleteQueuesOnStop(boolean deleteQueuesOnStop) {
    this.deleteQueuesOnStop = deleteQueuesOnStop;
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
      if (!state.startsWith("stop")) {
        log.error(e.getMessage(), e);
      }
    } catch (ExecutionException e) {
      if (!state.startsWith("stop")) {
        log.error(e.getMessage(), e);
      }
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
      cloudSessions.put(session.getId(), storeId);
      if (log.isDebugEnabled()) {
        log.debug("Saved session " + session.getId());
      }
      sendEvent("touch", session.getId().getBytes());
      replicateSession(session);
    }
  }

  public void replicateSession(Session session) throws IOException {
    // Replicate this session elsewhere
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType("application/octet-stream");
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("type", "replicate");
    headers.put("source", storeId);

    Channel mqChannel = mqConnection.createChannel();
    SessionSerializer serializer = new InternalSessionSerializer();
    serializer.setSession(session);
    mqChannel.basicPublish(sourceEventsExchange,
        replicationEventsRoutingKey,
        props,
        serializer.serialize());
    mqChannel.close();
  }

  @Override
  public void start() throws LifecycleException {
    setState("starting");
    super.start();
    if (log.isDebugEnabled()) {
      log.debug("Starting CloudStore: " + storeId);
    }

    try {
      ConnectionParameters cparams = new ConnectionParameters();
      cparams.setUsername(mqUser);
      cparams.setPassword(mqPassword);
      cparams.setVirtualHost(mqVirtualHost);
      mqConnection = new ConnectionFactory(cparams).newConnection(mqHost, mqPort);
      Channel mqChannel = mqConnection.createChannel();

      // Messages bound for all nodes in cluster go here
      mqChannel.exchangeDeclare(eventsExchange, "fanout", true);
      mqChannel.queueDeclare(eventsQueue, true);
      mqChannel.queueBind(eventsQueue, eventsExchange, "");

      // Messages bound for just this node go here
      mqChannel.exchangeDeclare(sourceEventsExchange, "direct", true);
      mqChannel.queueDeclare(sourceEventsQueue, true);
      String sourceEventsRoutingKey = sourceEventsRoutingPrefix + storeId;
      mqChannel.queueBind(sourceEventsQueue, sourceEventsExchange, sourceEventsRoutingKey);

      // Replication events
      mqChannel.exchangeDeclare(replicationEventsExchange, "topic", true);
      mqChannel.queueDeclare(replicationEventsQueue, true);
      mqChannel.queueBind(replicationEventsQueue, replicationEventsExchange, "session.replication");

      listenerPool.submit(new SessionEventListener());

      // Start several handlers to keep throughput high
      for (int i = 0; i < maxMqHandlers; i++) {
        UpdateEventHandler updateHandler = new UpdateEventHandler();
        workerPool.submit(updateHandler);

        LoadEventHandler loadHandler = new LoadEventHandler();
        workerPool.submit(loadHandler);
      }

      // Keep the session loader pool clear of dead loaders
      long timeout = ((long) (loadTimeout * 1000));
      timer.scheduleAtFixedRate(new SessionLoaderScavenger(), timeout, timeout);

    } catch (IOException e) {
      log.error(e.getMessage(), e);
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
    setState("started");
  }

  @Override
  public void stop() throws LifecycleException {
    setState("stopping");
    try {
      Channel mqChannel = mqConnection.createChannel();
      if (deleteQueuesOnStop) {
        mqChannel.queueDelete(eventsQueue);
      }
      if (deleteQueuesOnStop) {
        mqChannel.queueDelete(sourceEventsQueue);
      }
      if (deleteQueuesOnStop) {
        mqChannel.queueDelete(replicationEventsQueue);
      }
      // Make sure local sessions are replicated off this server
      for (Session session : localSessions.values()) {
        replicateSession(session);
      }
      // Force handlers to stop
      for (int i = 0; i < maxMqHandlers; i++) {
        updateEvents.add(null);
        loadEvents.add(null);
      }
      mqConnection.close();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    Registry.getRegistry(null, null).unregisterComponent(objectName);
    workerPool.shutdownNow();
    listenerPool.shutdownNow();
    setState("stopped");
  }

  protected void sendEvent(String type, byte[] body) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType("text/plain");
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("type", type);
    headers.put("source", storeId);
    props.setHeaders(headers);
    Channel mqChannel = mqConnection.createChannel();
    mqChannel.basicPublish(eventsExchange, "", props, body);
    mqChannel.close();
  }

  protected class DaemonThreadFactory implements ThreadFactory {

    protected ThreadGroup workersGroup = new ThreadGroup("cloud-sessions");
    protected AtomicInteger count = new AtomicInteger(0);

    public ThreadGroup getWorkersGroup() {
      return workersGroup;
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(workersGroup, r);
      t.setDaemon(true);
      t.setName("cloud-worker-" + count.incrementAndGet());

      return t;
    }
  }

  protected class SessionEventListener implements Callable<SessionEventListener> {

    Channel channel;
    BlockingQueue<QueueingConsumer.Delivery> incoming = new LinkedBlockingQueue<QueueingConsumer.Delivery>();
    QueueingConsumer eventsConsumer;
    QueueingConsumer sourceEventsConsumer;
    QueueingConsumer replicationEventsConsumer;

    protected SessionEventListener() throws IOException {
      channel = mqConnection.createChannel();
      eventsConsumer = new QueueingConsumer(channel, incoming);
      sourceEventsConsumer = new QueueingConsumer(channel, incoming);
      replicationEventsConsumer = new QueueingConsumer(channel, incoming);
      if (log.isDebugEnabled()) {
        log.debug("Consuming events on queue " + eventsQueue);
      }
      channel.basicConsume(eventsQueue, false, "events." + storeId, eventsConsumer);
      channel.basicConsume(sourceEventsQueue, false, "source." + storeId, sourceEventsConsumer);
      channel.basicConsume(replicationEventsQueue, false, "replication." + storeId, replicationEventsConsumer);
    }

    public SessionEventListener call() throws Exception {
      QueueingConsumer.Delivery delivery;
      while (null != (delivery = incoming.take())) {
        try {
          Map<String, Object> headers = delivery.getProperties().getHeaders();
          if (log.isDebugEnabled()) {
            log.debug("********************************* INCOMING *********************************");
            log.debug("Envelope  : " + delivery.getEnvelope().toString());
            log.debug("Properties: " + delivery.getProperties().toString());
            String contentType = delivery.getProperties().getContentType();
            if (null == contentType || !contentType.equals("application/octet-stream")) {
              log.debug("Body      : " + new String(delivery.getBody()));
            }
            log.debug("********************************* /INCOMING ********************************");
          }
          if (headers.containsKey("source")) {
            String source = headers.get("source").toString();
            if (headers.containsKey("type")) {
              String id;
              CloudSessionMessage msg;
              switch (CloudSession.Events.valueOf(headers.get("type").toString().toUpperCase())) {
                case TOUCH:
                  id = new String(delivery.getBody());
                  if (cloudSessions.containsKey(id)) {
                    String idSource = cloudSessions.get(id);
                    synchronized (idSource) {
                      cloudSessions.put(id, source);
                    }
                  } else {
                    cloudSessions.put(id, source);
                  }
                  if (log.isDebugEnabled()) {
                    log.debug("Node " + source + " claiming session " + id);
                  }
                  break;
                case DESTROY:
                  id = new String(delivery.getBody());
                  String idSource = cloudSessions.get(id);
                  synchronized (idSource) {
                    cloudSessions.remove(id);
                  }
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
                  try {
                    loadEvents.put(msg);
                  } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                  }
                  break;
                case UPDATE:
                case REPLICATE:
                  if (log.isDebugEnabled()) {
                    log.debug("Received update event from " + source);
                  }
                  msg = new CloudSessionMessage();
                  msg.setType(headers.get("type").toString());
                  msg.setId(headers.get("id").toString());
                  msg.setBody(delivery.getBody());
                  updateEvents.add(msg);
                  break;
                case CLEAR:
                  synchronized (cloudSessions) {
                    cloudSessions.clear();
                  }
                  break;
                case GETALL:
                  try {
                    workerPool.submit(new GetAllEventHandler(source));
                  } catch (IOException e) {
                    log.error(e.getMessage(), e);
                  }
                  break;
                case GETIDS:
                  try {
                    workerPool.submit(new GetIdsEventHandler());
                  } catch (IOException e) {
                    log.error(e.getMessage(), e);
                  }
                  break;
              }
            }
          }
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Throwable t) {
          if (!state.startsWith("stop")) {
            log.error(t.getMessage(), t);
          }
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
        InternalSessionDeserializer deserializer = new InternalSessionDeserializer();
        deserializer.setSession(session);
        deserializer.setBytes(msg.getBody());

        // Use custom classloading so session attributes are preserved
        Container container = manager.getContainer();
        Loader loader = null;
        ClassLoader classLoader = null;
        if (null != container) {
          loader = container.getLoader();
          if (null != loader) {
            classLoader = loader.getClassLoader();
            deserializer.setClassLoader(classLoader);
          }
        }

        deserializer.deserialize();
        if (responseQueues.containsKey(session.getId())) {
          BlockingQueue<CloudSession> queue = responseQueues.get(session.getId());
          synchronized (queue) {
            responseQueues.remove(session.getId());
          }
          queue.add(session);
          SessionLoader sessionLoader = sessionLoaders.get(session.getId());
          synchronized (sessionLoader) {
            sessionLoaders.remove(session.getId());
          }
        }
        save(session);
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
          CloudSession session = localSessions.get(msg.getId());
          SessionSerializer serializer = new InternalSessionSerializer();
          serializer.setSession(session);
          msg.setBody(serializer.serialize());

          AMQP.BasicProperties props = new AMQP.BasicProperties();
          props.setContentType("application/octet-stream");
          Map<String, Object> headers = new LinkedHashMap<String, Object>();
          headers.put("type", "update");
          headers.put("source", storeId);
          headers.put("id", msg.getId());
          props.setHeaders(headers);

          mqChannel
              .basicPublish(sourceEventsExchange, sourceEventsRoutingPrefix + msg.getSource(), props, msg.getBody());

          synchronized (session) {
            localSessions.remove(msg.getId());
          }
        }
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
        props.setHeaders(headers);

        mqChannel.basicPublish(sourceEventsExchange,
            sourceEventsRoutingPrefix + cloudSessions.get(id),
            props,
            id.getBytes());
        responseQueues.put(id, responseQueue);
        sessionLoaders.put(id, this);
        CloudSession session = responseQueue.take();
        close();
        synchronized (this) {
          sessionLoaders.remove(id);
        }
        if (log.isDebugEnabled()) {
          log.debug("Session loader runtime: " + String.valueOf(((System.currentTimeMillis() - startTime) * .001)) + "s");
        }

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
      for (Map.Entry<String, SessionLoader> entry : sessionLoaders.entrySet()) {
        long runtime = (long) ((System.currentTimeMillis() - entry.getValue().getStartTime()) * .001);
        if (runtime > loadTimeout) {
          log.info("Scavenging dead session loader " + entry.getValue().toString() + " after " + runtime + " secs.");
          synchronized (entry.getValue().getResponseQueue()) {
            responseQueues.remove(entry.getKey()).add(null);
          }
          synchronized (entry.getValue()) {
            sessionLoaders.remove(entry.getKey());
          }
        }
      }
    }
  }
}
