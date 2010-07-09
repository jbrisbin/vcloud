package com.jbrisbin.vcloud.cache.test;

import com.jbrisbin.vcloud.cache.AsyncCacheCallback;
import com.jbrisbin.vcloud.cache.RabbitMQAsyncCache;
import com.jbrisbin.vcloud.cache.RabbitMQAsyncCacheProvider;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.*;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class AsyncCacheProviderTest {

  static final String ALPHA = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  static final int NUM_ALPHA = ALPHA.length();
  static final Random rand = new Random();
  static Logger log = LoggerFactory.getLogger( AsyncCacheProviderTest.class );
  static ApplicationContext spring;
  static RabbitMQAsyncCacheProvider cacheProvider;
  static RabbitMQAsyncCache cache;
  static ConnectionFactory factory;
  static Connection connection;
  static int runs = 5;
  static CountDownLatch countDown = new CountDownLatch( runs );

  long totalTime = 0;
  long minTime = 0;
  long maxTime = 0;

  @BeforeClass
  public static void start() throws IOException {
    spring = new ClassPathXmlApplicationContext(
        "com/jbrisbin/vcloud/cache/test/async-cache.xml" );
    cacheProvider = spring.getBean( RabbitMQAsyncCacheProvider.class );
    factory = spring.getBean( ConnectionFactory.class );
    connection = factory.newConnection();

    cache = spring.getBean( RabbitMQAsyncCache.class );
  }

  @Test
  public void testStoreObject() throws IOException {
    TestObject obj = new TestObject();
    StringBuffer data = new StringBuffer();
    int size = rand.nextInt( 128 );
    for ( int i = 0; i < size; i++ ) {
      data.append( ALPHA.charAt( rand.nextInt( NUM_ALPHA ) ) );
    }
    obj.setData( data.toString() );
    cache.add( "test.object", obj );

    try {
      Thread.sleep( 1000 );
    } catch ( InterruptedException e ) {
    }
  }

  @Test
  public void testLoadObject() throws InterruptedException {
    for ( int i = 0; i < runs; i++ ) {
      cache.load( "test.object", new AsyncCacheCallback() {

        long startTime = System.currentTimeMillis();

        @Override
        public void onObjectLoad( Object obj ) {
          long endTime = System.currentTimeMillis();
          long interval = (endTime - startTime);
          totalTime += interval;
          if ( minTime == 0 || interval < minTime ) {
            minTime = interval;
          } else if ( maxTime == 0 || interval > maxTime ) {
            maxTime = interval;
          }
          log.info( "Got object " + obj + " in " + interval + "ms" );
          countDown.countDown();
        }

        @Override
        public void onError( Throwable t ) {
          log.error( t.getMessage(), t );
        }
      } );
    }
    countDown.await();
    log.info( "Average time: " + new Double( (totalTime / runs) ) );
    log.info( "Max time: " + maxTime );
    log.info( "Min time: " + minTime );
  }

  @AfterClass
  public static void stop() throws IOException {
    connection.close();
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

  static class TestObject implements Serializable {

    private String data;

    public String getData() {
      return data;
    }

    public void setData( String data ) {
      this.data = data;
    }

    @Override
    public String toString() {
      String s = super.toString();
      return s + ",data=" + data.substring( 0, 16 ) + "...";
    }

  }

}
