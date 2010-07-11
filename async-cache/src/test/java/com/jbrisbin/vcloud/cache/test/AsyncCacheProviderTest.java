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

import java.io.IOException;
import java.io.Serializable;
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
  static int runs = 50;
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
  public void testStoreAndLoadObject() throws IOException, InterruptedException {
    for ( int i = 0; i < runs; i++ ) {
      TestObject obj = new TestObject();
      StringBuffer data = new StringBuffer();
      int size = rand.nextInt( 128 );
      for ( int j = 0; j < size; j++ ) {
        data.append( ALPHA.charAt( rand.nextInt( NUM_ALPHA ) ) );
      }
      obj.setData( data.toString() );
      String id = "test.object." + i;
      cache.add( id, obj );
    }
    Thread.sleep( 1000 );

    for ( int i = 0; i < runs; i++ ) {
      final String id = "test.object." + i;
      cache.load( id, new AsyncCacheCallback() {

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
          log.info( "Waiting on " + countDown.getCount() + " more..." );
        }

        @Override
        public void onError( Throwable t ) {
          log.error( t.getMessage(), t );
        }
      } );
      //Thread.sleep( 300 );
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

  static class TestObject implements Serializable {

    private String data;

    public String getData() {
      return data;
    }

    public void setData( String data ) {
      this.data = data;
    }

    @Override
    public boolean equals( Object obj ) {
      if ( obj instanceof TestObject ) {
        return ((TestObject) obj).getData().equals( data );
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      String s = super.toString();
      return s + ",data=" + (data.length() > 16 ? data.substring( 0, 16 ) : data) + "...";
    }

  }

}
