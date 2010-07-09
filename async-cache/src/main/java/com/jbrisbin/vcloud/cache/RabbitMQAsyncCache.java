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

package com.jbrisbin.vcloud.cache;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RabbitMQAsyncCache implements AsyncCache {

  protected final Logger log = LoggerFactory.getLogger( getClass() );
  protected final boolean debug = log.isDebugEnabled();

  protected String id;
  @Autowired
  protected ConnectionFactory connectionFactory;
  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  protected Connection connection = null;
  /**
   * Name of the topic exchange to use for requesting objects.
   */
  protected String objectRequestExchange = "amq.topic";

  /**
   * Name of the fanout exchange used for sending heartbeat messages.
   */
  protected String heartbeatExchange = "amq.fanout";
  /**
   * How often to send out the heartbeat.
   */
  protected long heartbeatInterval = 3000L;
  /**
   * Is this cache active?
   */
  protected AtomicBoolean active = new AtomicBoolean( false );
  /**
   * Tasks that are currently running.
   */
  protected List<Future<?>> activeTasks = new ArrayList<Future<?>>();
  /**
   * Set of cache IDs for our cacheNodes. Used to determine if all responses have returned or
   * not.
   */
  protected ConcurrentSkipListSet<String> cacheNodes = new ConcurrentSkipListSet<String>();
  /**
   * Number of cacheNodes we expect responses from.
   */
  protected AtomicInteger numOfCacheNodes = new AtomicInteger( 1 );
  /**
   * Timer for issuing delayed tasks.
   */
  protected Timer delayTimer = new Timer( true );
  /**
   * How many Loaders to run concurrently.
   */
  protected int maxWorkers = 3;
  /**
   * How long to wait for exepcted responses.
   */
  protected long loadTimeout = 3000L;
  /**
   * Event handlers waiting on objects to be loaded.
   */
  protected ConcurrentSkipListMap<String, List<AsyncCacheCallback>> objectLoadCallbacks = new ConcurrentSkipListMap<String, List<AsyncCacheCallback>>();
  protected BlockingQueue<QueueingConsumer.Delivery> loadResponses = new LinkedBlockingQueue<QueueingConsumer.Delivery>();
  protected QueueingConsumer loadConsumer;

  public RabbitMQAsyncCache() {
  }

  @Override
  public void setId( String id ) {
    this.id = id;
  }

  @Override
  public String getId() {
    return this.id;
  }

  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  public void setConnectionFactory( ConnectionFactory connectionFactory ) {
    this.connectionFactory = connectionFactory;
  }

  public String getObjectRequestExchange() {
    return objectRequestExchange;
  }

  public void setObjectRequestExchange( String objectRequestExchange ) {
    this.objectRequestExchange = objectRequestExchange;
  }

  public String getHeartbeatExchange() {
    return heartbeatExchange;
  }

  public void setHeartbeatExchange( String heartbeatExchange ) {
    this.heartbeatExchange = heartbeatExchange;
  }

  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval( long heartbeatInterval ) {
    this.heartbeatInterval = heartbeatInterval;
  }

  public int getMaxWorkers() {
    return maxWorkers;
  }

  public void setMaxWorkers( int maxWorkers ) {
    this.maxWorkers = maxWorkers;
  }

  public long getLoadTimeout() {
    return loadTimeout;
  }

  public void setLoadTimeout( long loadTimeout ) {
    this.loadTimeout = loadTimeout;
  }

  @Override
  public void add( String id, Object obj ) {
    add( id, obj, Long.MAX_VALUE );
  }

  @Override
  public void add( String id, Object obj, long expiry ) {
    try {
      byte[] body = serialize( obj );
      workerPool.submit( new SendEvent( "store", objectRequestExchange, id, body ) );
    } catch ( IOException e ) {
      log.error( e.getMessage(), e );
    }
  }

  @Override
  public void setParent( String childId, String parentId ) {
    throw new IllegalAccessError( "This method is not yet implemented" );
  }

  @Override
  public void remove( String id ) {
    workerPool.submit( new SendEvent( "clear", objectRequestExchange, "id", new byte[0] ) );
  }

  @Override
  public void remove( String id, long delay ) {
    SendEvent event = new SendEvent( "clear", objectRequestExchange, id, new byte[0] );
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put( "expiration", delay );
    event.properties.setHeaders( headers );
    workerPool.submit( event );
  }

  @Override
  public void load( String id, final AsyncCacheCallback callback ) {
    if ( objectLoadCallbacks.containsKey( id ) ) {
      objectLoadCallbacks.get( id ).add( callback );
    } else {
      List<AsyncCacheCallback> callbacks = new ArrayList<AsyncCacheCallback>();
      callbacks.add( callback );
      objectLoadCallbacks.put( id, callbacks );
    }
    workerPool.submit( new SendEvent( "load", objectRequestExchange, id, new byte[0] ) );
  }

  @Override
  public void clear() {
    workerPool.submit( new SendEvent( "clear", objectRequestExchange, "#", new byte[0] ) );
  }

  @Override
  public void start() {
    active.set( true );

    try {
      Channel channel = getConnection().createChannel();
      channel.exchangeDeclare( objectRequestExchange, "topic", true, false, null );
      channel.queueDeclare( id, true, false, true, null );

      loadConsumer = new QueueingConsumer( channel, loadResponses );
      channel.basicConsume( id, loadConsumer );
    } catch ( IOException e ) {
      log.error( e.getMessage(), e );
    }

    activeTasks.add( workerPool.submit( new HeartbeatMonitor() ) );
    workerPool.submit( new SendEvent( "ping", heartbeatExchange, "", new byte[0] ) );
    delayTimer.scheduleAtFixedRate( new TimerTask() {
      @Override
      public void run() {
        if ( cacheNodes.size() > 0 ) {
          numOfCacheNodes.set( cacheNodes.size() );
        }
      }
    }, 0, heartbeatInterval );

    // For loading objects
    for ( int i = 0; i < maxWorkers; i++ ) {
      activeTasks.add( workerPool.submit( new ObjectLoadMonitor() ) );
    }
  }

  @Override
  public void stop() {
    stop( true );
  }

  @Override
  public void stop( boolean interruptIfRunning ) {
    active.set( false );
    for ( Future<?> f : activeTasks ) {
      f.cancel( interruptIfRunning );
    }
    if ( interruptIfRunning ) {
      workerPool.shutdownNow();
    } else {
      workerPool.shutdown();
    }
  }

  @Override
  public boolean isActive() {
    return active.get();
  }

  @Override
  public void setActive( boolean active ) {
    this.active.set( active );
  }

  protected Connection getConnection() throws IOException {
    if ( null == connection ) {
      connection = connectionFactory.newConnection();
    }
    return connection;
  }

  protected byte[] serialize( Object obj ) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream( bytesOut );
    oout.writeObject( obj );
    oout.flush();
    oout.close();
    bytesOut.flush();
    bytesOut.close();

    return bytesOut.toByteArray();
  }

  protected Object deserialize( byte[] bytes ) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bytesIn = new ByteArrayInputStream( bytes );
    ObjectInputStream oin = new ObjectInputStream( bytesIn );
    Object obj = oin.readObject();
    oin.close();
    bytesIn.close();

    if ( debug ) {
      log.debug( "Deserialized " + bytes.length + " bytes for object " + obj );
    }

    return obj;
  }

  class HeartbeatMonitor implements Runnable {
    @Override
    public void run() {
      Channel heartbeatChannel = null;
      try {
        heartbeatChannel = getConnection().createChannel();
        QueueingConsumer heartbeatConsumer = new QueueingConsumer( heartbeatChannel );
        heartbeatChannel.basicConsume( id, false, heartbeatConsumer );

        while ( true ) {
          QueueingConsumer.Delivery delivery = heartbeatConsumer.nextDelivery();
          AMQP.BasicProperties properties = delivery.getProperties();
          String type = properties.getType();
          if ( "ping".equals( type ) ) {
            // We don't respond to PING requests since we're a client
          } else if ( "pong".equals( type ) ) {
            byte[] body = delivery.getBody();
            if ( body.length > 0 ) {
              String cacheId = new String( delivery.getBody() );
              if ( null != cacheId && cacheId instanceof String ) {
                cacheNodes.add( cacheId );
              }
            }
          }
        }
      } catch ( InterruptedException e ) {
        // IGNORED
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      } finally {
        try {
          heartbeatChannel.close();
        } catch ( Throwable t ) {
        }
      }
    }
  }

  class ObjectLoadMonitor implements Runnable {
    @Override
    public void run() {
      try {
        while ( true ) {
          QueueingConsumer.Delivery delivery = loadResponses.take();
          long startTime = System.currentTimeMillis();
          AMQP.BasicProperties properties = delivery.getProperties();
          String type = properties.getType();
          String objectId = properties.getCorrelationId();
          if ( "response".equals( type ) ) {
            byte[] body = delivery.getBody();
            Object obj;
            if ( body.length > 0 ) {
              try {
                if ( debug ) {
                  long interval = System.currentTimeMillis() - startTime;
                  log.debug( "Before deserialize at " + interval + "ms" );
                }
                obj = deserialize( delivery.getBody() );
              } catch ( ClassNotFoundException e ) {
                log.error( e.getMessage(), e );
                obj = e;
              } catch ( IOException e ) {
                log.error( e.getMessage(), e );
                obj = e;
              }
            } else {
              obj = new NullObject();
            }
            if ( debug ) {
              long interval = System.currentTimeMillis() - startTime;
              log.debug( "Before callbacks at " + interval + "ms" );
            }
            List<AsyncCacheCallback> callbacks = objectLoadCallbacks.get( objectId );
            synchronized (callbacks) {
              if ( null != callbacks ) {
                for ( AsyncCacheCallback callback : callbacks ) {
                  if ( obj instanceof Throwable ) {
                    callback.onError( (Throwable) obj );
                  } else {
                    callback.onObjectLoad( obj );
                  }
                }
                callbacks.clear();
              }
            }
          } else {
            log.warn( "Invalid message type: '" + type + "': " + properties );
          }

          long endTime = System.currentTimeMillis();
          if ( debug ) {
            long interval = endTime - startTime;
            log.debug( "Load response processed in " + interval + "ms" );
          }
        }
      } catch ( InterruptedException e ) {
      }
    }
  }

  class NullObject {
    // To represent <NULL>
  }

  class SendEvent implements Runnable {

    String event;
    String exchange;
    String routingKey;
    byte[] body;
    AMQP.BasicProperties properties = new AMQP.BasicProperties();

    SendEvent( String event, String exchange, String routingKey, byte[] body ) {
      this.event = event;
      this.exchange = exchange;
      this.routingKey = routingKey;
      this.body = body;

      properties.setType( event );
      properties.setReplyTo( id );
    }

    @Override
    public void run() {
      Channel sendChannel = null;
      try {
        sendChannel = getConnection().createChannel();
        sendChannel.basicPublish( exchange, routingKey, properties, body );
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      } finally {
        try {
          sendChannel.close();
        } catch ( Throwable t ) {
        }
      }

    }
  }

}
