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
import org.apache.tomcat.util.modeler.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

  static enum Mode {
    ONEFORALL, REPLICATED
  }

  protected Logger log = LoggerFactory.getLogger(getClass());
  protected boolean DEBUG = log.isDebugEnabled();
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
  protected String replicationEventsExchange = null;
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
   * Name of the queue on which this store will listen for events.
   */
  protected String sourceEventsQueue = null;
  /**
   * Name of the exchange for loading/saving sessions.
   */
  protected String sessionEventsExchange = "amq.topic";
  /**
   * Prefix to prepend on session IDs to get queue name.
   */
  protected String sessionEventsQueuePattern = null;
  /**
   * Use only one RabbitMQ connection, though each worker and listener has its own Channel.
   */
  protected Connection mqConnection;
  /**
   * The shared channel used by miscellaneous functions in the Store.
   */
  protected Channel mqChannel;
  /**
   * The length of time (in seconds) until a loader is considered dead.
   */
  protected long loadTimeout = 15;
  /**
   * What mode to operate in. One of "allforone" or "replicated".
   */
  protected Mode operationMode = Mode.REPLICATED;
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
   * Listeners are message dispatchers. Having several of these means higher throughputs at the expense of more server
   * resources.
   */
  protected ExecutorService listenerPool = Executors
      .newCachedThreadPool(new DaemonThreadFactory("listeners", "listener-"));
  /**
   * Workers pull events from the following Queues and do work, so they have their own ThreadPool.
   */
  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  /**
   * Keep references to submitted workers in case we need to cancel them all.
   */
  protected List<Future> workers = new ArrayList<Future>();
  /**
   * Update and replication events are dispatched to this Queue.
   */
  protected LinkedBlockingQueue<CloudSessionMessage> updateEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  /**
   * Load requests are dispatched to this Queue.
   */
  protected LinkedBlockingQueue<CloudSessionMessage> loadEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  /**
   * Queue for deleteing local sessions.
   */
  protected LinkedBlockingQueue<String> deleteEvents = new LinkedBlockingQueue<String>();
  /**
   * List of what sessions are valid throughout the cloud.
   */
  protected ConcurrentSkipListSet<String> sessions = new ConcurrentSkipListSet<String>();
  /**
   * Map of the actual session objects.
   */
  protected ConcurrentSkipListMap<String, CloudSession> localSessions = new ConcurrentSkipListMap<String, CloudSession>();
  /**
   * The loaders put themselves in this Map so we can sweep it periodically and keep dead loaders from building up.
   */
  protected ConcurrentSkipListMap<String, SessionLoader> sessionLoaders = new ConcurrentSkipListMap<String, SessionLoader>();
  /**
   * The maximum number of times to attempt to load the session.
   */
  protected int maxRetries = 3;

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

  public String getSourceEventsQueue() {
    return sourceEventsQueue;
  }

  public void setSourceEventsQueue(String sourceEventsQueue) {
    this.sourceEventsQueue = sourceEventsQueue;
  }

  public String getSessionEventsExchange() {
    return sessionEventsExchange;
  }

  public void setSessionEventsExchange(String sessionEventsExchange) {
    this.sessionEventsExchange = sessionEventsExchange;
  }

  public String getSessionEventsQueuePattern() {
    return sessionEventsQueuePattern;
  }

  public void setSessionEventsQueuePattern(String sessionEventsQueuePattern) {
    this.sessionEventsQueuePattern = sessionEventsQueuePattern;
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

  public void setOperationMode(String opMode) {
    this.operationMode = Mode.valueOf(opMode.toUpperCase());
  }

  public Mode getOperationMode() {
    return this.operationMode;
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

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
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
    return sessions.size();
  }

  /**
   * What session IDs exist anywhere in the cloud?
   *
   * @return
   * @throws IOException
   */
  public String[] keys() throws IOException {
    return sessions.toArray(new String[sessions.size()]);
  }

  public boolean isValidSession(String id) {
    if (sessions.contains(id)) {
      Session session = localSessions.get(id);
      if (null != session) {
        return session.isValid();
      }
      return true;
    }
    return false;
  }

  public void processExpires() {
    try {
      for (String id : keys()) {
        if (!isValidSession(id)) {
          if (DEBUG) {
            log.debug("Session " + id + " has expired.");
          }
          remove(id);
        }
      }
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
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
    // Check locally first
    CloudSession session = localSessions.get(id);
    if (null != session) {
      if (DEBUG) {
        log.debug("Found a local session for: " + id);
      }
      return session;
    }

    // Load from the cloud
    SessionLoader loader;
    if (sessions.contains(id)) {
      if (DEBUG) {
        log.debug("Loading session from the cloud: " + id);
      }
      Future f = null;
      for (int i = 0; i < maxRetries; i++) {
        if (DEBUG && i > 0) {
          log.debug("********* Load attempt: " + (i + 1));
        }
        if (null == (loader = sessionLoaders.get(id))) {
          loader = new SessionLoader(id);
          sessionLoaders.put(id, loader);
          f = workerPool.submit(loader);
        }
        session = loader.getSession();
        if (null == session) {
          if (DEBUG) {
            log.debug(" ***** SESSION LOADER TIMEOUT! *****");
            log.debug("Loader: " + loader.toString());
            log.debug("Cloud: " + sessions.toString());
            log.debug("Local: " + localSessions.toString());
          }
        } else {
          if (DEBUG) {
            double runtime = ((System.currentTimeMillis() - loader.getStartTime()) * .001);
            log.debug("Loader runtime: " + new DecimalFormat("#.###s").format(runtime));
          }
          break;
        }
      }
      if (null != f) {
        sessionLoaders.remove(id);
        if (!f.isDone()) {
          f.cancel(true);
        }
      }
    }

    return session;
  }

  /**
   * Remove this session ID from the cloud by sending out a "destroy" message, which causes every node to delete this
   * session ID from its membership.
   *
   * @param id
   * @throws IOException
   */

  public void remove(String id) throws IOException {
    sessions.remove(id);
    localSessions.remove(id);
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
   * @param session
   * @throws IOException
   */
  public void save(Session session) throws IOException {
    String id = session.getId();
    if (sessions.add(id)) {
      // This is a new session.
      String qname = String.format(sessionEventsQueuePattern, id);
      Channel channel = getMqChannel();
      synchronized (channel) {
        channel.queueBind(sourceEventsQueue, sessionEventsExchange, qname);
      }
      sendEvent("touch", id.getBytes());
      replicateSession(session);
    }
    localSessions.put(id, (CloudSession) session);
  }

  public void processDeleteEvent(String sessionId) {
    if (deleteEvents.remove(sessionId)) {
      localSessions.remove(sessionId);
    }
  }

  /**
   * Basically an "update" event.
   *
   * @param session
   * @throws IOException
   */
  public void replicateSession(Session session) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType("application/octet-stream");
    props.setReplyTo(sourceEventsQueue);
    props.setType("replicate");
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("id", session.getId());

    SessionSerializer serializer = new InternalSessionSerializer();
    serializer.setSession(session);
    byte[] bytes = serializer.serialize();

    Channel channel = getMqChannel();
    synchronized (channel) {
      channel.basicPublish(replicationEventsExchange, replicationEventsRoutingKey, props, bytes);
      channel.basicPublish(sessionEventsExchange,
          String.format(sessionEventsQueuePattern, session.getId()),
          props,
          bytes);
    }
  }

  public void replicateAttribute(CloudSession session, String attr) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType("application/octet-stream");
    props.setReplyTo(sourceEventsQueue);
    props.setType("setattr");
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("id", session.getId());
    headers.put("attribute", attr);
    props.setHeaders(headers);

    AttributeSerializer ser = new InternalAttributeSerializer();
    ser.setObject(session.getAttribute(attr));
    byte[] bytes = ser.serialize();

    Channel channel = getMqChannel();
    synchronized (channel) {
      channel.basicPublish(replicationEventsExchange, replicationEventsRoutingKey, props, bytes);
      channel.basicPublish(sessionEventsExchange, String.format(sessionEventsQueuePattern, session.getId()), props,
          bytes);
    }
  }

  public void removeAttribute(CloudSession session, String attr) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType("application/octet-stream");
    props.setReplyTo(sourceEventsQueue);
    props.setType("delattr");
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("id", session.getId());
    props.setHeaders(headers);

    Channel channel = getMqChannel();
    synchronized (channel) {
      channel.basicPublish(replicationEventsExchange, replicationEventsRoutingKey, props, attr.getBytes());
      channel.basicPublish(sessionEventsExchange, String.format(sessionEventsQueuePattern, session.getId()), props,
          attr.getBytes());
    }
  }

  @Override
  public void start() throws LifecycleException {
    setState("starting");
    MDC.put("method", "start()");
    super.start();
    if (DEBUG) {
      log.debug("Starting CloudStore: " + storeId);
    }

    try {
      mqChannel = getMqChannel();

      synchronized (mqChannel) {
        // Messages bound for all nodes in cluster go here
        mqChannel.exchangeDeclare(eventsExchange, "fanout", true);
        mqChannel.queueDeclare(eventsQueue, true, false, false, null);
        mqChannel.queueBind(eventsQueue, eventsExchange, "");

        // Messages bound for just this node go here
        mqChannel.queueDeclare(sourceEventsQueue, true, false, false, null);

        // Session events
        mqChannel.exchangeDeclare(sessionEventsExchange, "topic", true);

        // Replication events
        if (null != replicationEventsExchange && !replicationEventsExchange.trim().equals("")) {
          mqChannel.exchangeDeclare(replicationEventsExchange, "topic", true);
          mqChannel.queueDeclare(replicationEventsQueue, true, false, false, null);
          mqChannel.queueBind(replicationEventsQueue, replicationEventsExchange, replicationEventsRoutingKey);
        }
      }

      startWorkers();

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
    MDC.remove("method");
    setState("started");
  }

  @Override
  public void stop() throws LifecycleException {
    setState("stopping");
    MDC.put("method", "stop()");
    try {
      // Make sure local sessions are replicated off this server
      for (Session session : localSessions.values()) {
        replicateSession(session);
        localSessions.remove(session.getId());
      }

      // Wait until all sessions are replicated
      while (localSessions.size() > 0) {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        }
      }

      synchronized (this) {
        if (deleteQueuesOnStop) {
          getMqChannel();
          mqChannel.queueDelete(eventsQueue);
          mqChannel.queueDelete(sourceEventsQueue);
          mqChannel.queueDelete(replicationEventsQueue);
        }
        mqChannel.close();
        mqConnection.close();
      }
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    // Remove ourself from JMX
    Registry.getRegistry(null, null).unregisterComponent(objectName);

    // Stop worker threads
    stopWorkers();
    MDC.remove("method");
    setState("stopped");
  }

  protected void stopWorkers() {
    for (Future f : workers) {
      f.cancel(true);
    }
    try {
      listenerPool.shutdownNow();
    } catch (Throwable t) {
      // IGNORED
    }
    try {
      workerPool.shutdownNow();
    } catch (Throwable t) {
      // IGNORED
    }
    workers.clear();
  }

  protected void startWorkers() throws IOException {
    for (int i = 0; i < maxMqHandlers; i++) {
      workers.add(workerPool.submit(new EventListener(eventsQueue)));
      workers.add(workerPool.submit(new EventListener(sourceEventsQueue)));
      workers.add(workerPool.submit(new EventListener(replicationEventsQueue)));
      workers.add(workerPool.submit(new UpdateEventHandler()));
      workers.add(workerPool.submit(new LoadEventHandler()));
    }
  }

  protected void restartListeners() throws IOException {
    Channel channel = getMqChannel();
    synchronized (channel) {
      for (String id : localSessions.keySet()) {
        String qname = String.format(sessionEventsQueuePattern, id);
        channel.queueBind(sourceEventsQueue, sessionEventsExchange, qname);
      }
    }
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
    props.setReplyTo(sourceEventsQueue);
    props.setType(type);
    synchronized (mqChannel) {
      mqChannel.basicPublish(eventsExchange, "", props, body);
    }
  }

  protected void sendEventTo(String event, String source, byte[] body) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType("text/plain");
    props.setReplyTo(sourceEventsQueue);
    props.setType(event);
    synchronized (mqChannel) {
      mqChannel.basicPublish("", source, props, body);
    }
  }

  protected synchronized Connection getMqConnection() throws IOException {
    if (null == mqConnection) {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(mqHost);
      factory.setPort(mqPort);
      factory.setUsername(mqUser);
      factory.setPassword(mqPassword);
      factory.setVirtualHost(mqVirtualHost);
      factory.setRequestedHeartbeat(10);
      mqConnection = factory.newConnection();
    }
    return mqConnection;
  }

  protected synchronized Channel getMqChannel() throws IOException {
    if (null == mqChannel) {
      if (DEBUG) {
        log.debug("Creating a new RabbitMQ channel...");
      }
      mqChannel = getMqConnection().createChannel();
    }
    return mqChannel;
  }

  protected AttributeDeserializer getAttributeDeserializer(byte[] bytes) {
    AttributeDeserializer deserializer = new InternalAttributeDeserializer();
    deserializer.setBytes(bytes);
    Container container = manager.getContainer();
    Loader loader;
    ClassLoader classLoader;
    if (null != container) {
      loader = container.getLoader();
      if (null != loader) {
        classLoader = loader.getClassLoader();
        deserializer.setClassLoader(classLoader);
      }
    }
    return deserializer;
  }

  /**
   * A custom <b>ThreadFactory</b> implementation that uses a somewhat meaningful naming scheme to make troubleshooting
   * easier.
   */
  protected class DaemonThreadFactory implements ThreadFactory {

    protected String threadPrefix;
    protected ThreadGroup workersGroup;
    protected AtomicInteger count = new AtomicInteger(0);

    public DaemonThreadFactory(String groupName, String threadPrefix) {
      workersGroup = new ThreadGroup(groupName);
      this.threadPrefix = threadPrefix;
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(workersGroup, r);
      t.setDaemon(true);
      t.setName(threadPrefix + count.incrementAndGet());
      return t;
    }

  }

  /**
   * Dispatch incoming message "events" to the various workers.
   */
  protected class EventListener implements Runnable {

    Channel channel;
    QueueingConsumer eventsConsumer;

    public EventListener(String queue) {
      try {
        channel = getMqConnection().createChannel();
        eventsConsumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, true, eventsConsumer);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

    public void run() {
      while (true) {
        try {
          QueueingConsumer.Delivery delivery = eventsConsumer.nextDelivery();
          MDC.put("method", delivery.getProperties().getType() + ".delivery");
          Map<String, Object> headers = delivery.getProperties().getHeaders();
          String source = delivery.getProperties().getReplyTo();
          if (DEBUG) {
            log.debug(" ***** INCOMING " + String.format("%s", delivery.getProperties().getType())
                .toUpperCase() + " [" + source + "]: " + delivery.getProperties().toString());
          }
          String id;
          CloudSessionMessage msg;
          String attr;
          CloudSession session;
          switch (CloudSession.asEvent(delivery.getProperties().getType())) {
            case TOUCH:
              id = new String(delivery.getBody());
              sessions.add(id);
              if (operationMode.equals(Mode.ONEFORALL) && !source.equals(sourceEventsQueue)) {
                if (sessions.contains(id)) {
                  if (DEBUG) {
                    log.debug("Removing locally-cached copy of " + id);
                  }
                  //deleteEvents.add( id );
                }
              }
              break;
            case DESTROY:
              id = new String(delivery.getBody());
              try {
                workerPool.submit(new DestroyEventHandler(id));
              } catch (IOException e) {
                log.error(e.getMessage(), e);
              }
              break;
            case LOAD:
              id = new String(delivery.getBody());
              msg = new CloudSessionMessage();
              msg.setType("load");
              msg.setSource(source);
              msg.setId(id);
              loadEvents.add(msg);
              break;
            case UPDATE:
            case REPLICATE:
              if (!source.equals(sourceEventsQueue)) {
                String type = delivery.getProperties().getType();
                if (DEBUG) {
                  log.debug(type.toUpperCase() + " from " + source);
                }
                msg = new CloudSessionMessage();
                msg.setType(type);
                msg.setId(headers.get("id").toString());
                msg.setBody(delivery.getBody());
                msg.setSource(source);
                updateEvents.add(msg);
              }
              break;
            case CLEAR:
              if (DEBUG) {
                log.debug("Clearing all sessions.");
              }
              localSessions.clear();
              break;
            case GETALL:
              try {
                workerPool.submit(new GetAllEventHandler(source)).get();
              } catch (ExecutionException e) {
                log.error(e.getMessage(), e);
              }
              break;
            case SETATTR:
              if (!source.equals(sourceEventsQueue)) {
                id = headers.get("id").toString();
                attr = headers.get("attribute").toString();
                session = localSessions.get(id);
                if (null != session) {
                  try {
                    AttributeDeserializer deser = getAttributeDeserializer(delivery.getBody());
                    Object obj = deser.deserialize();
                    session.maybeSetAttributeInternal(attr, obj);
                  } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                  }
                }
              }
              break;
            case DELATTR:
              if (!source.equals(sourceEventsQueue)) {
                id = headers.get("id").toString();
                attr = new String(delivery.getBody());
                session = localSessions.get(id);
                if (null != session) {
                  session.maybeRemoveAttributeInternal(attr);
                }
              }
              break;
          }
          MDC.remove("method");
        } catch (InterruptedException e) {
          // Only DEBUG these, as they're generated on shutdown
          log.debug(e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Responsible for deserializing user sessions and dispatching them to the waiting response queues, or just keeping a
   * copy of them as a replica.
   */
  protected class UpdateEventHandler implements Runnable {

    protected Channel channel;

    public UpdateEventHandler() throws IOException {
      this.channel = getMqConnection().createChannel();
    }

    public void run() {
      while (true) {
        CloudSessionMessage sessionMessage;
        try {
          sessionMessage = updateEvents.take();
          if (DEBUG) {
            log.debug("************************ Update event: " + sessionMessage.toString());
          }
          MDC.put("method", "processUpdateEvent()");
          CloudSession session = (CloudSession) manager.createEmptySession();
          InternalSessionDeserializer deserializer = new InternalSessionDeserializer(session);
          deserializer.setBytes(sessionMessage.getBody());

          // Use custom classloading so session attributes are preserved
          // ADAPTED FROM: from org.apache.catalina.session.FileStore.load()
          Container container = manager.getContainer();
          Loader loader;
          ClassLoader classLoader;
          if (null != container) {
            loader = container.getLoader();
            if (null != loader) {
              classLoader = loader.getClassLoader();
              deserializer.setClassLoader(classLoader);
            }
          }
          try {
            deserializer.deserialize();
            session.access();
            String id = session.getId();
            SessionLoader sessLoader;
            if (null != (sessLoader = sessionLoaders.remove(id))) {
              if (DEBUG) {
                log.debug("Giving deserizlied session to: " + sessLoader.toString());
              }
              if (operationMode == Mode.REPLICATED) {
                session.setReplica(false);
                localSessions.put(id, session);
              }
              sessLoader.getSessions().offer(session);
            } else if (sessionMessage.getType().equals("replicate")) {
              if (operationMode == Mode.REPLICATED) {
                session.setReplica(true);
              }
              if (localSessions.containsKey(id)) {
                localSessions.put(id, session);
              }
            }
            session.endAccess();
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
        } catch (InterruptedException e) {
          log.debug("Interrupting " + this.toString() + ": " + e.getMessage());
        }
        MDC.remove("method");
      }
    }
  }

  /**
   * Responsible for serializing user sessions and sending them back to the requestor.
   */
  protected class LoadEventHandler implements Runnable {

    Channel channel;

    public LoadEventHandler() {
      try {
        channel = getMqConnection().createChannel();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

    public void run() {
      while (true) {
        try {
          CloudSessionMessage sessionMessage = loadEvents.take();
          if (DEBUG) {
            log.debug("************************ Load event: " + sessionMessage.toString());
          }
          MDC.put("method", "processLoadEvent()");
          String id = sessionMessage.getId();

          AMQP.BasicProperties props = new AMQP.BasicProperties();
          props.setContentType("application/octet-stream");
          props.setReplyTo(sourceEventsQueue);
          props.setType("update");
          Map<String, Object> headers = new LinkedHashMap<String, Object>();
          headers.put("id", sessionMessage.getId());
          props.setHeaders(headers);

          CloudSession session = localSessions.get(id);
          byte[] bytes = new byte[0];
          if (null != session) {
            if (DEBUG) {
              log.debug("Serializing session " + (null != session ? session.toString() : "<NULL>"));
            }
            SessionSerializer serializer = new InternalSessionSerializer();
            serializer.setSession(session);
            bytes = serializer.serialize();
            if (DEBUG) {
              log.debug("Sending message: " + props.toString());
            }
          } else {
            log.warn(" *** WARNING! *** Asked to load a non-local session: " + id);
          }
          synchronized (channel) {
            channel.basicPublish("", sessionMessage.getSource(), props, bytes);
          }
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
          log.debug("Interrupting " + this.toString() + ": " + e.getMessage());
        }
        MDC.remove("method");
      }
    }

    public void close() {
      try {
        channel.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

  }

  /**
   * Not used at the moment, but is intended for maintenance/status apps that need to know about every session
   * throughout the cloud.
   */
  protected class GetAllEventHandler implements Runnable {

    protected String source;

    public GetAllEventHandler(String source) {
      this.source = source;
    }

    public void run() {
      for (Map.Entry<String, CloudSession> entry : localSessions.entrySet()) {
        CloudSessionMessage msg = new CloudSessionMessage();
        msg.setType("load");
        msg.setSource(source);
        msg.setId(entry.getKey());
        loadEvents.add(msg);
      }
    }

  }

  protected class TouchEventHandler implements Runnable {

    protected String id;
    protected String source;

    public TouchEventHandler(String id, String source) throws IOException {
      this.id = id;
      this.source = source;
    }

    public void run() {
      try {
        if (sessions.add(id) && localSessions.containsKey(id)) {
          // This is a new session.
          String qname = String.format(sessionEventsQueuePattern, id);
          synchronized (mqChannel) {
            mqChannel.queueBind(sourceEventsQueue, sessionEventsExchange, qname);
          }
        }
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  protected class DestroyEventHandler implements Runnable {

    protected String id;
    protected Channel channel;

    public DestroyEventHandler(String id) throws IOException {
      this.id = id;
      this.channel = getMqConnection().createChannel();
    }

    public void run() {
      try {
        sessions.remove(id);
        String qname = String.format(sessionEventsQueuePattern, id);
        channel.queueUnbind(sourceEventsQueue, sessionEventsExchange, qname);
      } catch (Throwable t) {
        log.debug(t.getMessage());
      } finally {
        try {
          channel.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Responsible for pretending to be synchronously loading a user session from wherever the object actually resides.
   */
  protected class SessionLoader implements Runnable {

    String id;
    Channel channel;
    CloudSession session;
    ArrayBlockingQueue<CloudSession> sessions = new ArrayBlockingQueue<CloudSession>(1);
    long startTime;

    public SessionLoader(String id) throws IOException {
      this.id = id;
      this.channel = getMqConnection().createChannel();
    }

    /**
     * We need to know how long this loader has been trying to load this session.
     *
     * @return
     */
    public long getStartTime() {
      return startTime;
    }

    public CloudSession getSession() {
      if (null == session) {
        try {
          session = sessions.poll(loadTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        }
      }
      return session;
    }

    public ArrayBlockingQueue<CloudSession> getSessions() {
      return sessions;
    }

    public void run() {
      startTime = System.currentTimeMillis();
      AMQP.BasicProperties props = new AMQP.BasicProperties();
      props.setReplyTo(sourceEventsQueue);
      props.setType("load");
      try {
        channel.basicPublish(sessionEventsExchange, String.format(sessionEventsQueuePattern, id), props,
            id.getBytes());
      } catch (IOException e) {
        log.debug(e.getMessage());
      } finally {
        try {
          channel.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }
    }
  }

}
