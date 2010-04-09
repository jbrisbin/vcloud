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
 * The workhorse and backbone of the cloud session manager. This <b>Store</b> implementation manages a dynamic list of
 * references to session objects that exist on various nodes within the cloud.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class CloudStore extends StoreBase {

  /**
   * Info on this implementation of a <b>Store</b>.
   */
  static final String info = "CloudStore/1.0";
  /**
   * Name of this implementation.
   */
  static final String name = "CloudStore";

  protected Log log = LogFactory.getLog(getClass());
  /**
   * <b>ObjectName</b> we'll register ourself under in JMX so we can interact directly with the store.
   */
  protected ObjectName objectName;
  /**
   * Keep track of our internal state while starting and stopping so a few Exception catches won't be surprised if
   * things start blowing up.
   */
  protected String state = "stopped";
  /**
   * Hostname of the RabbitMQ server we want to connect to. A combination of setting different MQ servers, virtual
   * hosts, and exchanges gives us the flexibility to configure what "cluster" this node is a part of.
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
  /**
   * Number of simultaneous event handlers to create. The workers themselves use a cached thread pool, so this isn't a
   * direct reflection of the number of extra threads the Store will create. But having a higher number of workers
   * ensures that heavy message throughput can be adequately handled by the store.
   */
  protected int maxMqHandlers = 2;
  /**
   * Name of the fanout exchange to which events intended for the entire cloud are published.
   */
  protected String eventsExchange = "amq.fanout";
  /**
   * Name of the queue bound to the <b>eventsExchange</b>. This should probably be explicitly named using a standard
   * convention rather than auto-generating a name.
   */
  protected String eventsQueue = null;
  /**
   * Replication events are basically update events with the exception that they don't have a corresponding loader
   * sitting there, waiting for the session to be deserialized. Since we probably will want additional listeners
   * responsible for replication user sessions, there is a separate topic exchange just for replication events.
   */
  protected String replicationEventsExchange = "amq.topic";
  /**
   * Name of the queue this store creates and binds to the replication exchange.
   */
  protected String replicationEventsQueue = null;
  /**
   * Routing key which all other stores will use when publishing replication requests. Replication happens only to
   * randomly-selected nodes and not to everyone in the cloud.
   */
  protected String replicationEventsRoutingKey = "vcloud.session.replication";
  /**
   * Source events are anything intended to be processed by a specific node.
   */
  protected String sourceEventsExchange = "amq.direct";
  /**
   * Name of the queue which this store will bind to the <b>sourceEventsExchange</b>.
   */
  protected String sourceEventsQueue = null;
  /**
   * This value will be prefixed to the value retrieved from the local session <b>Map</b> to create the routing key
   * needed to inform a node it should send the user session to us.
   */
  protected String sourceEventsRoutingPrefix = "vcloud.source.";
  /**
   * Use only one RabbitMQ connection, though each worker and listener has its own Channel.
   */
  protected Connection mqConnection;
  /**
   * The length of time (in seconds) until a loader is considered dead.
   */
  protected long loadTimeout = 15;
  /**
   * Should I clean up after myself and delete all my queues when this store shuts down?
   */
  protected boolean deleteQueuesOnStop = true;
  /**
   * Name of this store within the cloud. This should be unique throughout the cloud. How this is arrived at is a matter
   * of each cloud's architecture. In many cases, simply concatenating the server's IP address with a dash ('-') and the
   * Tomcat server's instance ID will work (e.g. add '-Dinstance.id=172.23.10.13-TC2' to $CATALINA_OPTS and use this
   * system property inside your Context configuration file).
   */
  protected String storeId;
  /**
   * Create a custom <b>ThreadFactory</b> that sets the name of threads for listeners and workers.
   */
  protected DaemonThreadFactory daemonThreadFactory = new DaemonThreadFactory();
  /**
   * Listeners are message dispatchers. Having several of these means higher throughputs at the expense of more server
   * resources.
   */
  protected ExecutorService listenerPool = Executors.newCachedThreadPool(daemonThreadFactory);
  /**
   * Workers pull events from the following Queues and do work, so they have their own ThreadPool.
   */
  protected ExecutorService workerPool = Executors.newCachedThreadPool(daemonThreadFactory);
  /**
   * Update and replication events are dispatched to this Queue.
   */
  protected BlockingQueue<CloudSessionMessage> updateEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  /**
   * Load requests are dispatched to this Queue.
   */
  protected BlockingQueue<CloudSessionMessage> loadEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  /**
   * Map of what session IDs are valid on any node in the cloud.
   */
  protected Map<String, String> cloudSessions = new HashMap<String, String>();
  /**
   * Map of the actual session objects.
   */
  protected Map<String, CloudSession> localSessions = new HashMap<String, CloudSession>();
  /**
   * These Queues are waiting on Session objects to be loaded.
   */
  protected Map<String, BlockingQueue<CloudSession>> responseQueues = new HashMap<String, BlockingQueue<CloudSession>>();
  /**
   * The loaders put themselves in this Map so we can sweep it periodically and keep dead loaders from building up.
   */
  protected HashMap<String, SessionLoader> sessionLoaders = new HashMap<String, SessionLoader>();
  /**
   * Periodically scan the <b>sessionLoaders</b> for "dead" loaders (i.e. loaders that have been attempting to load a
   * session longer than the <b>loadTimeout</b>.
   */
  protected Timer timer = new Timer();

  public CloudStore() {
  }

  /**
   * When the Store is starting and stopping, it might be useful for other threads to know that's happening if they
   * start catching Exceptions.
   *
   * @return
   */
  public String getState() {
    return state;
  }

  /**
   * What state this Store should be transitioned to. One of "stopped", "stopping", "started", "starting".
   *
   * @param state
   */
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

  /**
   * The <b>Manager</b> uses this method to get access to the internal Session Map.
   *
   * @return
   */
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

  /**
   * Retrieve a list of all session IDs (valid or not) on any node within the cloud.
   *
   * @return
   */
  public String[] getCloudSessionIds() {
    return cloudSessions.keySet().toArray(new String[cloudSessions.size()]);
  }

  /**
   * Retrieve a list of only those session IDs we consider "local".
   *
   * @return
   */
  public String[] getLocalSessionIds() {
    return localSessions.keySet().toArray(new String[localSessions.size()]);
  }

  /**
   * Simple getter for the master membership list (the Cloud Map).
   *
   * @return
   */
  public Map<String, String> getCloudSessionMap() {
    return cloudSessions;
  }

  /**
   * Get session loader timeout (in seconds).
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

  /**
   * How many sessions are there throughout the cloud?
   *
   * @return
   * @throws IOException
   */
  public int getSize() throws IOException {
    return cloudSessions.size();
  }

  /**
   * What session IDs exist anywhere in the cloud?
   *
   * @return
   * @throws IOException
   */
  public String[] keys() throws IOException {
    return cloudSessions.keySet().toArray(new String[getSize()]);
  }

  /**
   * Try to load the given session ID using a loader object that works in a separate thread.
   *
   * @param id
   * @return The Session object or null if this loader times out.
   * @throws ClassNotFoundException
   * @throws IOException
   */
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

  /**
   * Remove this session ID from the cloud by sending out a "destroy" message, which causes every node to delete this
   * session ID from its membership.
   *
   * @param id
   * @throws IOException
   */
  public void remove(String id) throws IOException {
    sendEvent("destroy", id.getBytes());
  }

  /**
   * This wipes out everything. Creates a complete blank slate everywhere in the cloud.
   *
   * @throws IOException
   */
  public void clear() throws IOException {
    sendEvent("clear", new byte[0]);
  }

  /**
   * Only clear local sessions. This method is likely only useful to JMX clients.
   */
  public void clearLocalSessions() {
    localSessions.clear();
  }

  /**
   * Save this session put keeping a copy in the <b>localSessions</b>, notify the rest of the cloud we're claiming
   * responsibility for this session, and replicate it to another node in case we go down.
   *
   * @param session
   * @throws IOException
   */
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

  /**
   * Basically an "update" event.
   *
   * @param session
   * @throws IOException
   */
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

  /**
   * Send a lightweight message "event" to everyone. Used for simple events like "touch", "clear", and "destroy"
   *
   * @param type
   * @param body
   * @throws IOException
   */
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

  /**
   * A custom <b>ThreadFactory</b> implementation that uses a somewhat meaningful naming scheme to make troubleshooting
   * easier.
   */
  protected class DaemonThreadFactory implements ThreadFactory {

    protected ThreadGroup workersGroup = new ThreadGroup("cloud-sessions");
    protected AtomicInteger count = new AtomicInteger(0);

    public Thread newThread(Runnable r) {
      Thread t = new Thread(workersGroup, r);
      t.setDaemon(true);
      t.setName("cloud-worker-" + count.incrementAndGet());
      return t;
    }
  }

  /**
   * Dispatch incoming messages ("event") to the various workers.
   */
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

    /**
     * Loop until a null is pulled from the incoming queue, inspect the message to find out where it came from and what
     * it wants done, and dispatch it to the right queue. It also handles little operations like "touch", "destroy" and
     * the like itself, because it doesn't need a separate Channel (and hence, a separate Thread) in which to operate.
     *
     * @return
     * @throws Exception
     */
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
                    // If someone else is claiming this, remove our copy
                    if (!idSource.equals(storeId) && localSessions.containsKey(id)) {
                      CloudSession session = localSessions.get(id);
                      if (!session.isReplica()) {
                        synchronized (session) {
                          localSessions.remove(id);
                        }
                      }
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
                  loadEvents.add(msg);
                  break;
                case UPDATE:
                case REPLICATE:
                  String type = headers.get("type").toString();
                  if (log.isDebugEnabled()) {
                    log.debug("Received " + type + " event from " + source);
                  }
                  msg = new CloudSessionMessage();
                  msg.setType(type);
                  msg.setId(headers.get("id").toString());
                  msg.setBody(delivery.getBody());
                  updateEvents.add(msg);
                  break;
                case CLEAR:
                  synchronized (cloudSessions) {
                    cloudSessions.clear();
                  }
                  synchronized (localSessions) {
                    localSessions.clear();
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
          // Not sure this should be blindly ACK'd, but it keeps the Queues clean this way.
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Throwable t) {
          if (!state.startsWith("stop")) {
            log.error(t.getMessage(), t);
          }
        }
      }
      close();
      return this;
    }

    /**
     * Close the Channel we've been using.
     */
    public void close() {
      try {
        channel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  /**
   * Responsible for deserializing user sessions and dispatching them to the waiting response queues, or just keeping a
   * copy of them as a replica.
   */
  protected class UpdateEventHandler implements Callable<UpdateEventHandler> {
    /**
     * Loop until null is returned from the <b>updateEvents</b> Queue and re-instantiate our session object. If there is
     * a loader waiting on this session, put it on the queue so the loader can finish doing what it was asked.
     *
     * @return
     * @throws Exception
     */
    public UpdateEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while (null != (msg = updateEvents.take())) {
        CloudSession session = (CloudSession) manager.createEmptySession();
        InternalSessionDeserializer deserializer = new InternalSessionDeserializer();
        deserializer.setSession(session);
        deserializer.setBytes(msg.getBody());

        // Use custom classloading so session attributes are preserved
        // ADAPTED FROM: from org.apache.catalina.session.FileStore.load()
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
          session.setReplica(false);
        } else {
          // Assume this is a replication
          session.setReplica(true);
        }
        save(session);
      }
      return this;
    }
  }

  /**
   * Responsible for serializing user sessions and sending them back to the requestor.
   */
  protected class LoadEventHandler implements Callable<LoadEventHandler> {

    protected Channel mqChannel = null;

    public LoadEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    /**
     * Loop until null is returned from the <b>loadEvents</b> Queue. Serialize the session into a message and sent it to
     * the requestor.
     *
     * @return
     * @throws Exception
     */
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

        }
      }
      close();
      return this;
    }

    /**
     * Close the Channel we've been using.
     */
    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  /**
   * Not used at the moment, but is intended for maintenance/status apps that need to know about every session
   * throughout the cloud.
   */
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

    /**
     * Close the Channel we've been using.
     */
    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  /**
   * Responsible for blasting out a touch message for every local session. This event usually happens when a new node is
   * started and it populates its Cloud Map by sending out a "getids" message.
   */
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

    /**
     * Close the Channel we've been using.
     */
    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  /**
   * Responsible for pretending to be synchronously loading a user session from wherever the object actually resides.
   */
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

    /**
     * We need to know how long this loader has been trying to load this session.
     *
     * @return
     */
    public long getStartTime() {
      return startTime;
    }

    /**
     * So we can either put a result on the queue, or pass it null, which causes the loop to exit normally.
     *
     * @return
     */
    public BlockingQueue<CloudSession> getResponseQueue() {
      return responseQueue;
    }

    /**
     * Pretend like we're synchronously loading a Session object from another server. Make my <b>responseQueue</b>
     * available to <b>UpdateEventHandler</b>s and make myself avaiable to the <b>SessionLoaderScavenger</b>.
     *
     * @return
     * @throws Exception
     */
    public CloudSession call() throws Exception {
      try {
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
      } finally {
        close();
      }
    }

    /**
     * Close the Channel we've been using.
     */
    public void close() {
      try {
        mqChannel.close();
      } catch (IOException e) {
        // IGNORED
      }
    }
  }

  /**
   * Responsible for making sure dead loaders don't build up.
   */
  protected class SessionLoaderScavenger extends TimerTask {
    /**
     * Check if a session loader has spent longer than <b>loadTimeout</b> trying to load a session.
     */
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
