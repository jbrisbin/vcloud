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
public class RabbitMQAsyncCacheProvider implements AsyncCache {

  protected final Logger log = LoggerFactory.getLogger( getClass() );
  protected final boolean debug = log.isDebugEnabled();

  /**
   * Cloud-unique ID for this cache node.
   */
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
   * Name of the queue for this cache node.
   */
  protected String cacheNodeQueueName = null;
  /**
   * Name of the fanout exchange used for sending heartbeat messages.
   */
  protected String heartbeatExchange = "amq.fanout";
  /**
   * How often to send out the heartbeat.
   */
  protected long heartbeatInterval = 3000L;

  // Internal objects
  protected AtomicBoolean active = new AtomicBoolean( false );
  /**
   * Tasks that are currently running.
   */
  protected List<Future<?>> activeTasks = new ArrayList<Future<?>>();
  /**
   * Set of cache IDs for our cacheNodes. Used to determine if all responses have returned or
   * not.
   */
  protected ConcurrentSkipListSet<String> peers = new ConcurrentSkipListSet<String>();
  /**
   * Number of cacheNodes we expect responses from.
   */
  protected AtomicInteger numOfPeers = new AtomicInteger( 0 );
  /**
   * Timer for issuing delayed tasks.
   */
  protected Timer delayTimer = new Timer( true );
  /**
   * How many ObjectMonitors to run concurrently.
   */
  protected int maxWorkers = 3;

  protected BlockingQueue<QueueingConsumer.Delivery> objectRequests = new LinkedBlockingQueue<QueueingConsumer.Delivery>();
  protected QueueingConsumer requestsConsumer;

  /**
   * Primary object cache.
   */
  protected final ConcurrentSkipListMap<String, CacheEntry> objectCache = new ConcurrentSkipListMap<String, CacheEntry>();

  public RabbitMQAsyncCacheProvider() {
  }

  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  public void setConnectionFactory( ConnectionFactory connectionFactory ) {
    this.connectionFactory = connectionFactory;
  }

  public String getCacheNodeQueueName() {
    return cacheNodeQueueName;
  }

  public void setCacheNodeQueueName( String cacheNodeQueueName ) {
    this.cacheNodeQueueName = cacheNodeQueueName;
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

  @Override
  public void setId( String id ) {
    this.id = id;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public void start() {
    active.set( true );

    try {
      Channel channel = getConnection().createChannel();
      channel.exchangeDeclare( objectRequestExchange, "topic", true, false, null );
      channel.queueDeclare( cacheNodeQueueName, true, false, true, null );
      channel.queueBind( cacheNodeQueueName, objectRequestExchange, "#" );
      requestsConsumer = new QueueingConsumer( channel, objectRequests );
      channel.basicConsume( cacheNodeQueueName, true, requestsConsumer );

      channel.exchangeDeclare( heartbeatExchange, "fanout", true, false, null );
    } catch ( IOException e ) {
      log.error( e.getMessage(), e );
    }

    /*
    activeTasks.add( workerPool.submit( new HeartbeatMonitor() ) );
    delayTimer.scheduleAtFixedRate( new TimerTask() {
      @Override
      public void run() {
        taskPool.execute( new SendEvent( "ping", heartbeatExchange, "", id.getBytes() ) );
      }
    }, 100, heartbeatInterval );
    delayTimer.scheduleAtFixedRate( new TimerTask() {
      @Override
      public void run() {
        numOfPeers.set( peers.size() );
      }
    }, heartbeatInterval, heartbeatInterval );
     */

    for ( int i = 0; i < maxWorkers; i++ ) {
      activeTasks.add( workerPool.submit( new ObjectMonitor() ) );
    }
  }

  @Override
  public void stop() {
    stop( true );
  }

  @Override
  public void stop( boolean waitForThreadsToComplete ) {
    active.set( false );
    for ( Future<?> f : activeTasks ) {
      f.cancel( !waitForThreadsToComplete );
    }
    if ( waitForThreadsToComplete ) {
      workerPool.shutdown();
    } else {
      workerPool.shutdownNow();
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

  @Override
  public void add( String id, Object obj ) {
    add( id, obj, Long.MAX_VALUE );
  }

  @Override
  public void add( String id, Object obj, long expiry ) {
    CacheEntry entry = new CacheEntry();
    entry.value = obj;
    entry.expiration = expiry;
    objectCache.put( id, entry );
  }

  @Override
  public void setParent( String childId, String parentId ) {
    CacheEntry child = objectCache.get( childId );
    CacheEntry parent = objectCache.get( parentId );
    if ( null != child && null != parent ) {
      child.parent = parentId;
    }
  }

  @Override
  public void remove( String id ) {
    objectCache.remove( id );
  }

  @Override
  public void remove( final String id, long delay ) {
    delayTimer.schedule( new TimerTask() {
      @Override
      public void run() {
        remove( id );
      }
    }, delay );
  }

  @Override
  public void load( String id, AsyncCacheCallback callback ) {
  }

  @Override
  public void clear() {
    objectCache.clear();
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

    return obj;
  }

  class SendEvent implements Runnable {

    String event;
    String exchange;
    String routingKey;
    byte[] body;

    SendEvent( String event, String exchange, String routingKey, byte[] body ) {
      this.event = event;
      this.exchange = exchange;
      this.routingKey = routingKey;
      this.body = body;
    }

    @Override
    public void run() {
      AMQP.BasicProperties properties = new AMQP.BasicProperties();
      properties.setType( event );

      Channel channel = null;
      try {
        channel = getConnection().createChannel();
        channel.basicPublish( exchange, routingKey, properties, body );
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      } finally {
        try {
          channel.close();
        } catch ( Throwable t ) {
        }
      }

    }
  }

  class ObjectSendEvent implements Runnable {

    String correlationId;
    String replyTo;
    Object obj;

    ObjectSendEvent( String correlationId, String replyTo, Object obj ) {
      this.correlationId = correlationId;
      this.replyTo = replyTo;
      this.obj = obj;
    }

    @Override
    public void run() {
      Channel channel = null;
      try {
        channel = getConnection().createChannel();
        AMQP.BasicProperties properties = new AMQP.BasicProperties();
        properties.setType( "response" );
        properties.setCorrelationId( correlationId );
        properties.setReplyTo( cacheNodeQueueName );

        byte[] body = (null != obj ? serialize( obj ) : new byte[0]);
        channel.basicPublish( "", replyTo, properties, body );
      } catch ( IOException e ) {
        log.error( e.getMessage(), e );
      } finally {
        try {
          channel.close();
        } catch ( IOException e ) {
        }
      }
    }
  }

  class HeartbeatMonitor implements Runnable {

    String queue;
    QueueingConsumer heartbeatConsumer;

    HeartbeatMonitor() {
    }

    @Override
    public void run() {
      Channel heartbeatChannel = null;
      try {
        heartbeatChannel = getConnection().createChannel();
        queue = heartbeatChannel.queueDeclare().getQueue();
        heartbeatChannel.queueBind( queue, heartbeatExchange, "" );
        heartbeatConsumer = new QueueingConsumer( heartbeatChannel );
        heartbeatChannel.basicConsume( queue, false, heartbeatConsumer );

        while ( true ) {
          QueueingConsumer.Delivery delivery = heartbeatConsumer.nextDelivery();
          AMQP.BasicProperties properties = delivery.getProperties();
          String type = properties.getType();
          if ( debug ) {
            log.debug( "Received " + type.toUpperCase() + " heartbeat message: " + properties
                .toString() );
          }
          if ( "ping".equals( type ) ) {
            workerPool.execute( new SendEvent( "pong", heartbeatExchange, "", id.getBytes() ) );
          } else if ( "pong".equals( type ) ) {
            byte[] body = delivery.getBody();
            if ( body.length > 0 ) {
              String cacheId = new String( delivery.getBody() );
              if ( null != cacheId && cacheId instanceof String ) {
                peers.add( cacheId );
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

  class ObjectMonitor implements Runnable {
    @Override
    public void run() {
      try {
        while ( true ) {
          QueueingConsumer.Delivery delivery = objectRequests.take();
          AMQP.BasicProperties properties = delivery.getProperties();
          String correlationId = properties.getCorrelationId();
          String type = properties.getType();
          String replyTo = properties.getReplyTo();
          String objectId = delivery.getEnvelope().getRoutingKey();
          if ( debug ) {
            log.debug( "Received " + type.toUpperCase() + " object message: " + properties
                .toString() );
          }
          if ( "store".equals( type ) ) {
            Object obj = null;
            try {
              try {
                obj = deserialize( delivery.getBody() );
              } catch ( IOException e ) {
                log.error( e.getMessage(), e );
              }
              if ( null != obj ) {
                Map<String, Object> headers = properties.getHeaders();
                if ( null != headers && headers.containsKey( "expiration" ) ) {
                  long expiry = Long.parseLong( headers.get( "expiration" ).toString() );
                  add( objectId, obj, expiry );
                } else {
                  add( objectId, obj );
                }
              } else {
                log.warn( "Won't add a NULL object: " + objectId );
              }
            } catch ( ClassNotFoundException e ) {
              log.error( e.getMessage(), e );
            }
          } else if ( "load".equals( type ) ) {
            CacheEntry entry = objectCache.get( objectId );
            Object obj = null;
            if ( null != entry ) {
              long interval = (System.currentTimeMillis() - entry.createdAt);
              if ( interval < entry.expiration ) {
                obj = entry.value;
              } else {
                remove( objectId );
              }
            } else {
              log.warn( "No object with ID " + objectId + " found" );
            }
            workerPool.execute(
                new ObjectSendEvent( objectId, replyTo, (null != obj ? obj : null) ) );
          } else if ( "clear".equals( type ) ) {
            if ( objectId.equals( "#" ) ) {
              objectCache.clear();
            } else {
              Map<String, Object> headers = properties.getHeaders();
              if ( null != headers && headers.containsKey( "expiration" ) ) {
                long expiry = Long.parseLong( headers.get( "expiration" ).toString() );
                remove( objectId, expiry );
              } else {
                remove( objectId );
              }
            }
          } else if ( "children".equals( type ) ) {
            List<Object> childIds = new ArrayList<Object>();
            for ( CacheEntry entry : objectCache.values() ) {
              if ( entry.parent.equals( objectId ) ) {
                childIds.add( entry.value );
              }
            }
          }
        }
      } catch ( InterruptedException e ) {
        // IGNORED
      }
    }
  }

  class CacheEntry {
    final long createdAt = System.currentTimeMillis();
    Object value;
    long expiration;
    String parent = null;
  }
}
