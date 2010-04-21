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

  static enum Mode {
    ONEFORALL, REPLICATED
  }

  protected Logger log = LoggerFactory.getLogger( getClass() );
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
   * What mode to operate in. One of "allforone" or "replicated".
   */
  protected Mode operationMode = Mode.ONEFORALL;
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
      .newCachedThreadPool( new DaemonThreadFactory( "listeners", "listener-" ) );
  /**
   * Workers pull events from the following Queues and do work, so they have their own ThreadPool.
   */
  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  protected List<Future> workers = new ArrayList<Future>();
  /**
   * Update and replication events are dispatched to this Queue.
   */
  protected LinkedBlockingDeque<CloudSessionMessage> updateEvents = new LinkedBlockingDeque<CloudSessionMessage>();
  /**
   * Load requests are dispatched to this Queue.
   */
  protected LinkedBlockingDeque<CloudSessionMessage> loadEvents = new LinkedBlockingDeque<CloudSessionMessage>();
  /**
   * Map of what session IDs are valid on any node in the cloud.
   */
  protected ConcurrentHashMap<String, String> cloudSessions = new ConcurrentHashMap<String, String>();
  /**
   * Map of the actual session objects.
   */
  protected ConcurrentHashMap<String, CloudSession> localSessions = new ConcurrentHashMap<String, CloudSession>();
  /**
   * The loaders put themselves in this Map so we can sweep it periodically and keep dead loaders from building up.
   */
  protected ConcurrentHashMap<String, SessionLoader> sessionLoaders = new ConcurrentHashMap<String, SessionLoader>();
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
  public synchronized void setState( String state ) {
    this.state = state;
  }

  public String getMqHost() {
    return mqHost;
  }

  public void setMqHost( String mqHost ) {
    this.mqHost = mqHost;
  }

  public int getMqPort() {
    return mqPort;
  }

  public void setMqPort( int mqPort ) {
    this.mqPort = mqPort;
  }

  public String getMqUser() {
    return mqUser;
  }

  public void setMqUser( String mqUser ) {
    this.mqUser = mqUser;
  }

  public String getMqPassword() {
    return mqPassword;
  }

  public void setMqPassword( String mqPassword ) {
    this.mqPassword = mqPassword;
  }

  public String getMqVirtualHost() {
    return mqVirtualHost;
  }

  public void setMqVirtualHost( String mqVirtualHost ) {
    this.mqVirtualHost = mqVirtualHost;
  }

  public int getMaxMqHandlers() {
    return maxMqHandlers;
  }

  public void setMaxMqHandlers( int maxMqHandlers ) {
    this.maxMqHandlers = maxMqHandlers;
  }

  public String getEventsExchange() {
    return eventsExchange;
  }

  public void setEventsExchange( String eventsExchange ) {
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

  public void setEventsQueue( String eventsQueue ) {
    this.eventsQueue = eventsQueue;
  }

  public String getReplicationEventsExchange() {
    return replicationEventsExchange;
  }

  public void setReplicationEventsExchange( String replicationEventsExchange ) {
    this.replicationEventsExchange = replicationEventsExchange;
  }

  public String getReplicationEventsQueue() {
    return replicationEventsQueue;
  }

  public void setReplicationEventsQueue( String replicationEventsQueue ) {
    this.replicationEventsQueue = replicationEventsQueue;
  }

  public String getSourceEventsExchange() {
    return sourceEventsExchange;
  }

  public void setSourceEventsExchange( String sourceEventsExchange ) {
    this.sourceEventsExchange = sourceEventsExchange;
  }

  public String getSourceEventsQueue() {
    return sourceEventsQueue;
  }

  public void setSourceEventsQueue( String sourceEventsQueue ) {
    this.sourceEventsQueue = sourceEventsQueue;
  }

  public String getSourceEventsRoutingPrefix() {
    return sourceEventsRoutingPrefix;
  }

  public void setSourceEventsRoutingPrefix( String sourceEventsRoutingPrefix ) {
    this.sourceEventsRoutingPrefix = sourceEventsRoutingPrefix;
  }

  public String getStoreId() {
    return storeId;
  }

  public void setStoreId( String storeId ) {
    this.storeId = storeId;
  }

  public int getUpdateEventsCount() {
    return updateEvents.size();
  }

  public int getLoadEventsCount() {
    return sessionLoaders.size();
  }

  public void setOperationMode( String opMode ) {
    this.operationMode = Mode.valueOf( opMode.toUpperCase() );
  }

  public Mode getOperationMode() {
    return this.operationMode;
  }

  /**
   * Retrieve a list of all session IDs (valid or not) on any node within the cloud.
   *
   * @return
   */
  public String[] getCloudSessionIds() {
    return cloudSessions.keySet().toArray( new String[cloudSessions.size()] );
  }

  /**
   * Retrieve a list of only those session IDs we consider "local".
   *
   * @return
   */
  public String[] getLocalSessionIds() {
    return localSessions.keySet().toArray( new String[localSessions.size()] );
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
  public void setLoadTimeout( long loadTimeout ) {
    this.loadTimeout = loadTimeout;
  }

  public boolean isDeleteQueuesOnStop() {
    return deleteQueuesOnStop;
  }

  public void setDeleteQueuesOnStop( boolean deleteQueuesOnStop ) {
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
    return cloudSessions.keySet().toArray( new String[getSize()] );
  }

  /**
   * Try to load the given session ID using a loader object that works in a separate thread.
   *
   * @param id
   * @return The Session object or null if this loader times out.
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public Session load( String id ) throws ClassNotFoundException, IOException {
    CloudSession session = null;
    if ( cloudSessions.containsKey( id ) ) {
      // Check locally first
      if ( localSessions.containsKey( id ) ) {
        if ( log.isDebugEnabled() ) {
          log.debug( "Found a local session for: " + id );
        }
        session = localSessions.get( id );
        if ( null != session && !session.isReplica() ) {
          if ( log.isDebugEnabled() ) {
            log.debug( "Session is replica? " + session.isReplica() );
          }
          return session;
        }
      } else if ( operationMode.equals( Mode.REPLICATED ) ) {
        log.debug( "No local session " + id + " in replicated mode!" );
        return null;
      }
      // Check if this session is already being loaded
      if ( sessionLoaders.containsKey( id ) ) {
        log.debug( "Using existing session loader for: " + id );
        try {
          session = sessionLoaders.get( id ).getResponseQueue().poll( loadTimeout, TimeUnit.SECONDS );
        } catch ( InterruptedException e ) {
          log.error( e.getMessage(), e );
        }
        return session;
      }

      // Load from the cloud
      try {
        log.debug( "Loading session from the cloud: " + id );
        session = workerPool.submit( new SessionLoader( id ) ).get();
      } catch ( InterruptedException e ) {
        log.error( e.getMessage(), e );
      } catch ( ExecutionException e ) {
        log.error( e.getMessage(), e );
      }

      // No session yet. Try the entire cloud.
      if ( null == session ) {
        try {
          session = workerPool.submit( new SessionLoader( id, false ) ).get();
        } catch ( InterruptedException e ) {
          log.error( e.getMessage(), e );
        } catch ( ExecutionException e ) {
          log.error( e.getMessage(), e );
        }
      }
    } else {
      if ( log.isDebugEnabled() ) {
        log.debug( "Session " + id + " doesn't look like a valid session!" );
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
  public void remove( String id ) throws IOException {
    sendEvent( "destroy", id.getBytes() );
  }

  /**
   * This wipes out everything. Creates a complete blank slate everywhere in the cloud.
   *
   * @throws IOException
   */
  public void clear() throws IOException {
    sendEvent( "clear", new byte[0] );
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
  public void save( Session session ) throws IOException {
    cloudSessions.put( session.getId(), storeId );
    localSessions.put( session.getId(), (CloudSession) session );
    if ( log.isDebugEnabled() ) {
      log.debug( "save(): Saved session " + session.getId() );
    }
    sendEvent( "touch", session.getId().getBytes() );
    if ( operationMode.equals( Mode.REPLICATED ) ) {
      replicateSession( session );
    }
  }

  /**
   * Basically an "update" event.
   *
   * @param session
   * @throws IOException
   */
  public void replicateSession( Session session ) throws IOException {
    // Replicate this session elsewhere
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType( "application/octet-stream" );
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put( "type", "replicate" );
    headers.put( "source", storeId );
    headers.put( "id", session.getId() );
    props.setHeaders( headers );

    SessionSerializer serializer = new InternalSessionSerializer();
    serializer.setSession( session );
    Channel mqChannel = mqConnection.createChannel();
    if ( operationMode.equals( Mode.REPLICATED ) ) {
      // Blast this to everyone
      mqChannel.basicPublish( eventsExchange, "", props, serializer.serialize() );
    } else {
      // Replicate off-node
      mqChannel.basicPublish( replicationEventsExchange, replicationEventsRoutingKey, props, serializer.serialize() );
    }
    mqChannel.close();
  }

  @Override
  public void start() throws LifecycleException {
    setState( "starting" );
    super.start();
    if ( log.isDebugEnabled() ) {
      log.debug( "Starting CloudStore: " + storeId );
    }

    try {
      ConnectionParameters cparams = new ConnectionParameters();
      cparams.setUsername( mqUser );
      cparams.setPassword( mqPassword );
      cparams.setVirtualHost( mqVirtualHost );
      mqConnection = new ConnectionFactory( cparams ).newConnection( mqHost, mqPort );

      Channel mqChannel = mqConnection.createChannel();

      // Messages bound for all nodes in cluster go here
      mqChannel.exchangeDeclare( eventsExchange, "fanout", true );
      mqChannel.queueDeclare( eventsQueue, true );
      mqChannel.queueBind( eventsQueue, eventsExchange, "" );

      // Messages bound for just this node go here
      mqChannel.exchangeDeclare( sourceEventsExchange, "direct", true );
      mqChannel.queueDeclare( sourceEventsQueue, true );
      String sourceEventsRoutingKey = sourceEventsRoutingPrefix + storeId;
      mqChannel.queueBind( sourceEventsQueue, sourceEventsExchange, sourceEventsRoutingKey );

      // Replication events
      mqChannel.exchangeDeclare( replicationEventsExchange, "topic", true );
      mqChannel.queueDeclare( replicationEventsQueue, true );
      mqChannel.queueBind( replicationEventsQueue, replicationEventsExchange, replicationEventsRoutingKey );

      mqChannel.close();

      for ( int i = 0; i < maxMqHandlers; i++ ) {
        workers.add( workerPool.submit( new EventListener( eventsQueue ) ) );
        workers.add( workerPool.submit( new EventListener( sourceEventsQueue ) ) );
        workers.add( workerPool.submit( new EventListener( replicationEventsQueue ) ) );
      }

      // Keep the session loader pool clear of dead loaders
      long timeout = ((long) (loadTimeout * 1000));
      timer.scheduleAtFixedRate( new SessionLoaderScavenger(), timeout, timeout );

    } catch ( IOException e ) {
      log.error( e.getMessage(), e );
    }

    try {
      sendEvent( "getids", new byte[0] );
    } catch ( IOException e ) {
      log.error( e.getMessage(), e );
    }

    try {
      objectName = new ObjectName( "vCloud:type=SessionStore,id=" + storeId );
      Registry.getRegistry( null, null ).registerComponent( this, objectName, null );
    } catch ( MalformedObjectNameException e ) {
      log.error( e.getMessage(), e );
    } catch ( Exception e ) {
      log.error( e.getMessage(), e );
    }
    setState( "started" );
  }

  @Override
  public void stop() throws LifecycleException {
    setState( "stopping" );
    try {
      // Make sure local sessions are replicated off this server
      for ( Session session : localSessions.values() ) {
        replicateSession( session );
      }

      Channel mqChannel = mqConnection.createChannel();
      if ( deleteQueuesOnStop ) {
        mqChannel.queueDelete( eventsQueue );
        mqChannel.queueDelete( sourceEventsQueue );
        mqChannel.queueDelete( replicationEventsQueue );
      }
      mqChannel.close();
      mqConnection.close();

    } catch ( IOException e ) {
      log.error( e.getMessage(), e );
    }

    // Remove ourself from JMX
    Registry.getRegistry( null, null ).unregisterComponent( objectName );

    // Stop worker threads
    for ( Future f : workers ) {
      f.cancel( true );
    }
    listenerPool.shutdownNow();
    workerPool.shutdownNow();

    setState( "stopped" );
  }

  /**
   * Send a lightweight message "event" to everyone. Used for simple events like "touch", "clear", and "destroy"
   *
   * @param type
   * @param body
   * @throws IOException
   */
  protected void sendEvent( String type, byte[] body ) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    props.setContentType( "text/plain" );
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put( "type", type );
    headers.put( "source", storeId );
    props.setHeaders( headers );
    Channel mqChannel = mqConnection.createChannel();
    mqChannel.basicPublish( eventsExchange, "", props, body );
    mqChannel.close();
  }

  /**
   * A custom <b>ThreadFactory</b> implementation that uses a somewhat meaningful naming scheme to make troubleshooting
   * easier.
   */
  protected class DaemonThreadFactory implements ThreadFactory {

    protected String threadPrefix;
    protected ThreadGroup workersGroup;
    protected AtomicInteger count = new AtomicInteger( 0 );

    public DaemonThreadFactory( String groupName, String threadPrefix ) {
      workersGroup = new ThreadGroup( groupName );
      this.threadPrefix = threadPrefix;
    }

    public Thread newThread( Runnable r ) {
      Thread t = new Thread( workersGroup, r );
      t.setDaemon( true );
      t.setName( threadPrefix + count.incrementAndGet() );
      return t;
    }

  }

  /**
   * Dispatch incoming message "events" to the various workers.
   */
  protected class EventListener implements Runnable {

    Channel channel;
    QueueingConsumer eventsConsumer;

    public EventListener( String queue ) {
      try {
        channel = mqConnection.createChannel();
        eventsConsumer = new QueueingConsumer( channel );
        channel.basicConsume( queue, true, eventsConsumer );
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      }
    }

    public void run() {
      while ( true ) {
        try {
          QueueingConsumer.Delivery delivery = eventsConsumer.nextDelivery();
          if ( null != delivery ) {
            Map<String, Object> headers = delivery.getProperties().getHeaders();
            if ( log.isDebugEnabled() ) {
              log.debug( " ***** INCOMING: " + delivery.getProperties().toString() );
            }
            if ( headers.containsKey( "source" ) ) {
              String source = headers.get( "source" ).toString();
              if ( headers.containsKey( "type" ) ) {
                String id;
                CloudSessionMessage msg;
                switch ( CloudSession.Events.valueOf( headers.get( "type" ).toString().toUpperCase() ) ) {
                  case TOUCH:
                    id = new String( delivery.getBody() );
                    if ( !storeId.equals( source ) ) {
                      cloudSessions.put( id, source );
                      if ( log.isDebugEnabled() ) {
                        log.debug( "Node " + source + " is claiming " + id );
                      }
                      if ( operationMode.equals( Mode.ONEFORALL ) ) {
                        // Delete this session locally if we're passing it around rather than keeping our own copy
                        if ( !source.equals( storeId ) ) {
                          if ( localSessions.containsKey( id ) ) {
                            if ( log.isDebugEnabled() ) {
                              log.debug( "Removing session from local cache: " + id );
                            }
                            localSessions.remove( id );
                          }
                        }
                      }
                    } else {
                      if ( log.isDebugEnabled() ) {
                        log.debug( "Ignoring touch request for local session " + id );
                      }
                    }
                    break;
                  case DESTROY:
                    id = new String( delivery.getBody() );
                    if ( log.isDebugEnabled() ) {
                      log.debug( "Delete session " + id );
                    }
                    cloudSessions.remove( id );
                    localSessions.remove( id );
                    break;
                  case LOAD:
                    id = new String( delivery.getBody() );
                    if ( !storeId.equals( source ) ) {
                      if ( log.isDebugEnabled() ) {
                        log.debug( "Received load request for " + id + " from " + source );
                      }
                      msg = new CloudSessionMessage();
                      msg.setType( "load" );
                      msg.setSource( source );
                      msg.setId( id );
                      workerPool.submit( new LoadEventHandler( msg ) );
                    } else {
                      log.debug( " **** Load event ignored for " + id );
                    }
                    break;
                  case UPDATE:
                  case REPLICATE:
                    if ( !source.equals( storeId ) ) {
                      String type = headers.get( "type" ).toString();
                      if ( log.isDebugEnabled() ) {
                        log.debug( "Received " + type + " event from " + source );
                      }
                      msg = new CloudSessionMessage();
                      msg.setType( type );
                      msg.setId( headers.get( "id" ).toString() );
                      msg.setBody( delivery.getBody() );
                      msg.setSource( source );
                      workerPool.submit( new UpdateEventHandler( msg ) );
                    }
                    break;
                  case CLEAR:
                    if ( log.isDebugEnabled() ) {
                      log.debug( "Clearing all sessions." );
                    }
                    cloudSessions.clear();
                    localSessions.clear();
                    break;
                  case GETALL:
                    try {
                      workerPool.submit( new GetAllEventHandler( source ) ).get();
                    } catch ( ExecutionException e ) {
                      log.error( e.getMessage(), e );
                    }
                    break;
                  case GETIDS:
                    try {
                      workerPool.submit( new GetIdsEventHandler() ).get();
                    } catch ( ExecutionException e ) {
                      log.error( e.getMessage(), e );
                    }
                    break;
                }
              }
            }
          }
        } catch ( InterruptedException e ) {
          log.error( e.getMessage(), e );
        }
      }
    }
  }

  /**
   * Responsible for deserializing user sessions and dispatching them to the waiting response queues, or just keeping a
   * copy of them as a replica.
   */
  protected class UpdateEventHandler implements Runnable {

    CloudSessionMessage sessionMessage;

    public UpdateEventHandler( CloudSessionMessage sessionMessage ) {
      this.sessionMessage = sessionMessage;
    }

    /**
     * Loop until null is returned from the <b>updateEvents</b> Queue and re-instantiate our session object. If there is
     * a loader waiting on this session, put it on the queue so the loader can finish doing what it was asked.
     */
    public void run() {
      CloudSession session = (CloudSession) manager.createEmptySession();
      InternalSessionDeserializer deserializer = new InternalSessionDeserializer();
      deserializer.setSession( session );
      deserializer.setBytes( sessionMessage.getBody() );

      // Use custom classloading so session attributes are preserved
      // ADAPTED FROM: from org.apache.catalina.session.FileStore.load()
      Container container = manager.getContainer();
      Loader loader = null;
      ClassLoader classLoader = null;
      if ( null != container ) {
        loader = container.getLoader();
        if ( null != loader ) {
          classLoader = loader.getClassLoader();
          deserializer.setClassLoader( classLoader );
        }
      }
      try {
        deserializer.deserialize();
        log.debug( "Deserialized session: " + session.toString() );
        if ( sessionLoaders.containsKey( session.getId() ) ) {
          if ( log.isDebugEnabled() ) {
            log.debug( "Giving deserizlied session to: " + session.getId() );
          }
          session.setReplica( false );
          sessionLoaders.get( session.getId() ).getResponseQueue().add( session );
        } else {
          // Assume this is a replication
          session.setReplica( true );
        }
        cloudSessions.put( session.getId(), sessionMessage.getSource() );
        localSessions.put( session.getId(), session );
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      }
    }
  }

  /**
   * Responsible for serializing user sessions and sending them back to the requestor.
   */
  protected class LoadEventHandler implements Runnable {

    protected CloudSessionMessage sessionMessage;

    public LoadEventHandler( CloudSessionMessage sessionMessage ) {
      this.sessionMessage = sessionMessage;
    }

    /**
     * Loop until null is returned from the <b>loadEvents</b> Queue. Serialize the session into a message and sent it to
     * the requestor.
     */
    public void run() {
      if ( localSessions.containsKey( sessionMessage.getId() ) ) {
        CloudSession session = localSessions.get( sessionMessage.getId() );
        if ( log.isDebugEnabled() ) {
          log.debug( "Serializing session " + (null != session ? session.toString() : "<NULL>") );
        }
        SessionSerializer serializer = new InternalSessionSerializer();
        serializer.setSession( session );
        try {
          sessionMessage.setBody( serializer.serialize() );

          AMQP.BasicProperties props = new AMQP.BasicProperties();
          props.setContentType( "application/octet-stream" );
          Map<String, Object> headers = new LinkedHashMap<String, Object>();
          headers.put( "type", "update" );
          headers.put( "source", storeId );
          headers.put( "id", sessionMessage.getId() );
          props.setHeaders( headers );

          if ( log.isDebugEnabled() ) {
            log.debug( "Sending message: " + props.toString() );
          }
          Channel mqChannel = mqConnection.createChannel();
          mqChannel
              .basicPublish( sourceEventsExchange, sourceEventsRoutingPrefix + sessionMessage.getSource(), props,
                  sessionMessage.getBody() );
          mqChannel.close();
        } catch ( IOException e ) {
          log.error( e.getMessage(), e );
        }
      }
    }

  }

  /**
   * Not used at the moment, but is intended for maintenance/status apps that need to know about every session
   * throughout the cloud.
   */
  protected class GetAllEventHandler implements Runnable {

    protected String source;

    public GetAllEventHandler( String source ) {
      this.source = source;
    }

    public void run() {
      try {
        Channel channel = mqConnection.createChannel();
        for ( Map.Entry<String, CloudSession> entry : localSessions.entrySet() ) {
          CloudSessionMessage msg = new CloudSessionMessage();
          msg.setType( "load" );
          msg.setSource( source );
          msg.setId( entry.getKey() );
          try {
            workerPool.submit( new LoadEventHandler( msg ) ).get();
          } catch ( InterruptedException e ) {
            log.error( e.getMessage(), e );
          } catch ( ExecutionException e ) {
            log.error( e.getMessage(), e );
          }
        }
        channel.close();
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      }
    }

  }

  /**
   * Responsible for blasting out a touch message for every local session. This event usually happens when a new node is
   * started and it populates its Cloud Map by sending out a "getids" message.
   */
  protected class GetIdsEventHandler implements Runnable {
    public void run() {
      for ( Map.Entry<String, CloudSession> entry : localSessions.entrySet() ) {
        try {
          sendEvent( "touch", entry.getKey().getBytes() );
        } catch ( IOException e ) {
          log.error( e.getMessage(), e );
        }
      }
    }
  }

  /**
   * Responsible for pretending to be synchronously loading a user session from wherever the object actually resides.
   */
  protected class SessionLoader implements Callable<CloudSession> {

    String id;
    Channel mqChannel;
    BlockingQueue<CloudSession> responseQueue = new LinkedBlockingQueue<CloudSession>();
    long startTime;
    boolean respectSource = true;

    public SessionLoader( String id ) {
      this( id, true );
    }

    protected SessionLoader( String id, boolean respectSource ) {
      this.id = id;
      startTime = System.currentTimeMillis();
      this.respectSource = respectSource;
      try {
        mqChannel = mqConnection.createChannel();
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      }
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
      String source = cloudSessions.get( id );
      if ( null == source || null == mqChannel ) {
        return null;
      }
      if ( operationMode.equals( Mode.REPLICATED ) ) {
        return localSessions.get( id );
      }

      CloudSession session;
      sessionLoaders.put( id, this );
      AMQP.BasicProperties props = new AMQP.BasicProperties();
      Map<String, Object> headers = new LinkedHashMap<String, Object>();
      headers.put( "type", "load" );
      headers.put( "source", storeId );
      props.setHeaders( headers );

      // Try and load from the specified source unless told otherwise
      if ( respectSource ) {
        String routingKey = (sourceEventsRoutingPrefix + source);
        if ( log.isDebugEnabled() ) {
          log.debug( "Sending load message to " + routingKey );
        }
        mqChannel.basicPublish( sourceEventsExchange, routingKey, props, id.getBytes() );
      } else {
        if ( log.isDebugEnabled() ) {
          log.debug( "Sending load message to cloud" );
        }
        mqChannel.basicPublish( eventsExchange, "", props, id.getBytes() );
      }

      // Wait for the response
      if ( log.isDebugEnabled() ) {
        log.debug( "Waiting for session load..." );
      }
      session = responseQueue.poll( loadTimeout, TimeUnit.SECONDS );
      sessionLoaders.remove( id );
      if ( null == session ) {
        log.warn( " ***** Session loader timed out! *****" );
        log.debug( cloudSessions.toString() );
        log.debug( localSessions.toString() );
      }

      if ( log.isDebugEnabled() ) {
        log.debug( "Session loader runtime: " + String
            .valueOf( ((System.currentTimeMillis() - startTime) * .001) ) + "s" );
      }

      mqChannel.close();
      return session;
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
      for ( Map.Entry<String, SessionLoader> entry : sessionLoaders.entrySet() ) {
        long runtime = (long) ((System.currentTimeMillis() - entry.getValue().getStartTime()) * .001);
        if ( runtime > loadTimeout ) {
          log.info( "Scavenging dead session loader " + entry.getValue().toString() + " after " + runtime + " secs." );
          sessionLoaders.remove( entry.getKey() );
        }
      }
    }
  }
}
