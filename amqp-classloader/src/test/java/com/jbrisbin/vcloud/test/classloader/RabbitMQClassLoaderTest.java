package com.jbrisbin.vcloud.test.classloader;

import com.jbrisbin.vcloud.classloader.RabbitMQClassLoader;
import com.jbrisbin.vcloud.classloader.RabbitMQResourceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RabbitMQClassLoaderTest {

  Logger log = LoggerFactory.getLogger(getClass());
  RabbitMQResourceProvider provider;
  String className = "com.rabbitmq.client.Connection";
  CountDownLatch countdown = new CountDownLatch(1);

  @Before
  public void start() throws IOException {
    log.info("Starting provider...");
    provider = new RabbitMQResourceProvider(getClass().getClassLoader());
    provider.start();
  }

  @After
  public void stop() {
    try {
      countdown.await(15, TimeUnit.SECONDS);
      log.info("Stopping provider...");
      provider.stop();
    } catch (InterruptedException e) {
      // IGNORED
    }
  }

  @Test
  public void testRemoteClassLoading() throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    RabbitMQClassLoader classLoader = new RabbitMQClassLoader(new URLClassLoader(new URL[0]));

    log.info("Loading class " + className + "...");
    Class<?> clazz = classLoader.loadClass(className);

    assert clazz != null;
    assert clazz.isAssignableFrom(Class.forName(className));

    countdown.countDown();
  }

}
