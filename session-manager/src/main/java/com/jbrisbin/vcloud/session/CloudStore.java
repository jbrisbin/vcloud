package com.jbrisbin.vcloud.session;

import com.rabbitmq.client.*;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.session.StoreBase;
import org.apache.catalina.util.Base64;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * User: jbrisbin
 * Date: Apr 2, 2010
 * Time: 5:20:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class CloudStore extends StoreBase {

  public static final String INFO = "CloudStore/1.0";
  public static final String NAME = "CloudStore";

  // RabbitMQ
  private String mqHost = "localhost";
  private int mqPort = 5672;
  private String mqUser = "guest";
  private String mqPassword = "guest";
  private String mqVirtualHost = "/";
  private int maxMqHandlers = 2;
  private String eventsExchange = "amq.fanout";
  private Connection mqConnection;

  // Unique store key to identify this node
  private String storeKey;

  private ExecutorService workerPool = Executors.newCachedThreadPool();
  private BlockingQueue<CloudSessionMessage> updateEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  private BlockingQueue<CloudSessionMessage> loadEvents = new LinkedBlockingQueue<CloudSessionMessage>();
  private BlockingQueue<CloudSessionMessage> replicationEvents = new LinkedBlockingQueue<CloudSessionMessage>();

  // Local/private sessions
  private ConcurrentHashMap<String, String> cloudSessions = new ConcurrentHashMap<String, String>();
  private ConcurrentHashMap<String, CloudSession> localSessions = new ConcurrentHashMap<String, CloudSession>();
  private ConcurrentHashMap<String, SessionLoader> sessionLoaders = new ConcurrentHashMap<String, SessionLoader>();

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

  public ConcurrentHashMap<String, CloudSession> getLocalSessions() {
    return localSessions;
  }

  @Override
  public String getInfo() {
    return INFO;
  }

  @Override
  public String getStoreName() {
    return NAME;
  }

  public int getSize() throws IOException {
    return cloudSessions.size();
  }

  public String[] keys() throws IOException {
    return cloudSessions.keySet().toArray( new String[getSize()] );
  }

  public Session load( String s ) throws ClassNotFoundException, IOException {
    try {
      return workerPool.submit( new SessionLoader() ).get();
    } catch ( InterruptedException e ) {
      manager.getContainer().getLogger().error( e.getMessage(), e );
    } catch ( ExecutionException e ) {
      manager.getContainer().getLogger().error( e.getMessage(), e );
    }
    return null;
  }

  public void remove( String id ) throws IOException {
    sendEvent( "destroy", id.getBytes() );
  }

  public void clear() throws IOException {
    sendEvent( "clear", new byte[0] );
  }

  public void save( Session session ) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void start() throws LifecycleException {
    super.start();
    storeKey = generateStoreKey();

    ConnectionParameters cparams = new ConnectionParameters();
    cparams.setUsername( mqUser );
    cparams.setPassword( mqPassword );
    cparams.setVirtualHost( mqVirtualHost );
    try {
      this.mqConnection = new ConnectionFactory( cparams ).newConnection( mqHost, mqPort );
    } catch ( IOException e ) {
      manager.getContainer().getLogger().error( e.getMessage(), e );
    }
  }

  @Override
  public void stop() throws LifecycleException {
    try {
      this.mqConnection.close();
    } catch ( IOException e ) {
      manager.getContainer().getLogger().error( e.getMessage(), e );
    }
  }

  private void sendEvent( String type, byte[] body ) throws IOException {
    AMQP.BasicProperties props = new AMQP.BasicProperties();
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put( "type", type );
    headers.put( "source", storeKey );
    props.setHeaders( headers );
    Channel mqChannel = mqConnection.createChannel();
    mqChannel.basicPublish( eventsExchange, "#", props, body );
    mqChannel.close();
  }

  private String generateStoreKey() {
    byte[] b = new byte[16];
    new Random().nextBytes( b );
    return new String( Base64.encode( b ) );
  }

  private void serializeSessionToMessage( CloudSessionMessage msg ) throws IOException {
    if ( localSessions.containsKey( msg.getId() ) ) {
      CloudSession session = localSessions.get( msg.getId() );
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      ObjectOutputStream objectOut = new ObjectOutputStream( bytesOut );
      session.writeObjectData( objectOut );
      objectOut.flush();
      objectOut.close();
      bytesOut.flush();
      bytesOut.close();
      localSessions.remove( msg.getId() );
      byte[] buff = bytesOut.toByteArray();
      msg.setBody( buff );
    } else {
      msg.setBody( new byte[0] );
    }
  }

  private CloudSession deserializeSessionFromMessage(
      CloudSessionMessage msg ) throws IOException, ClassNotFoundException {
    CloudSession session = (CloudSession) manager.createEmptySession();
    ByteArrayInputStream bytesIn = new ByteArrayInputStream( msg.getBody() );
    ObjectInputStream objectIn = new ObjectInputStream( bytesIn );
    session.readObjectData( objectIn );
    session.setManager( manager );
    localSessions.put( msg.getId(), session );
    return session;
  }

  private class SessionEventListener implements Callable<SessionEventListener> {

    private Channel mqChannel = null;
    private QueueingConsumer eventsConsumer;

    private SessionEventListener() throws IOException {
      mqChannel = mqConnection.createChannel();
      eventsConsumer = new QueueingConsumer( mqChannel );
    }

    public SessionEventListener call() throws Exception {
      // Loop forever, processing incoming events
      QueueingConsumer.Delivery delivery;
      while ( null != (delivery = eventsConsumer.nextDelivery()) ) {
        Map<String, Object> headers = delivery.getProperties().getHeaders();
        if ( headers.containsKey( "source" ) ) {
          String source = headers.get( "source" ).toString();
          if ( !source.equals( storeKey ) ) {
            if ( headers.containsKey( "type" ) ) {
              String id;
              CloudSessionMessage msg;
              switch ( CloudSession.Events.valueOf( headers.get( "type" ).toString().toUpperCase() ) ) {
                case CREATE:
                  id = new String( delivery.getBody() );
                  cloudSessions.put( id, source );
                  break;
                case DESTROY:
                  id = new String( delivery.getBody() );
                  cloudSessions.remove( id );
                  break;
                case LOAD:
                  id = new String( delivery.getBody() );
                  msg = new CloudSessionMessage();
                  msg.setType( headers.get( "type" ).toString() );
                  msg.setId( headers.get( "id" ).toString() );
                  String replyTo = delivery.getProperties().getReplyTo();
                  if ( null != replyTo && replyTo.contains( "/" ) ) {
                    String[] s = replyTo.split( "/" );
                    msg.setReplyToExchange( s[0] );
                    msg.setReplyToRoutingKey( s[1] );
                  }
                  loadEvents.put( msg );
                  break;
                case UPDATE:
                  msg = new CloudSessionMessage();
                  msg.setType( headers.get( "type" ).toString() );
                  msg.setId( headers.get( "id" ).toString() );
                  msg.setBody( delivery.getBody() );
                  updateEvents.add( msg );
                  break;
                case CLEAR:
                  cloudSessions.clear();
                  break;
                case REPLICATE:
                  break;
              }
            }
            mqChannel.basicAck( delivery.getEnvelope().getDeliveryTag(), false );
          }
        }
      }
      return this;
    }
  }

  private class UpdateEventHandler implements Callable<UpdateEventHandler> {

    private Channel mqChannel = null;

    public UpdateEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    public UpdateEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while ( null != (msg = updateEvents.take()) ) {
        CloudSession session = deserializeSessionFromMessage( msg );
        SessionLoader loader = sessionLoaders.remove( session.getId() );
        loader.getResponseQueue().add( session );
      }
      return this;
    }
  }

  private class LoadEventHandler implements Callable<LoadEventHandler> {

    private Channel mqChannel = null;

    public LoadEventHandler() throws IOException {
      mqChannel = mqConnection.createChannel();
    }

    public LoadEventHandler call() throws Exception {
      CloudSessionMessage msg;
      while ( null != (msg = loadEvents.take()) ) {
        serializeSessionToMessage( msg );

        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props.setContentType( "application/octet-stream" );
        Map<String, Object> headers = new LinkedHashMap<String, Object>();
        headers.put( "type", "update" );
        headers.put( "source", storeKey );
        headers.put( "id", msg.getId() );

        mqChannel.basicPublish( msg.getReplyToExchange(), msg.getReplyToRoutingKey(), props, msg.getBody() );
      }
      return this;
    }
  }

  private class SessionLoader implements Callable<CloudSession> {

    private BlockingQueue<CloudSession> responseQueue = new LinkedBlockingQueue<CloudSession>();

    public BlockingQueue<CloudSession> getResponseQueue() {
      return responseQueue;
    }

    public CloudSession call() throws Exception {
      return responseQueue.take();
    }
  }
}
