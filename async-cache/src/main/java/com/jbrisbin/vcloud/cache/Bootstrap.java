package com.jbrisbin.vcloud.cache;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class Bootstrap {

  static final Logger log = LoggerFactory.getLogger( Bootstrap.class );
  static Options opts = new Options();

  static {
    opts.addOption( "c", "config", true, "Configuration files for cache." );
  }

  public static void main( String[] args ) {

    CommandLineParser parser = new BasicParser();
    CommandLine cmdLine = null;
    try {
      cmdLine = parser.parse( opts, args );
    } catch ( ParseException e ) {
      log.error( e.getMessage(), e );
    }

    String configFile = "/etc/cloud/async-cache.xml";
    if ( null != cmdLine ) {
      if ( cmdLine.hasOption( 'c' ) ) {
        configFile = cmdLine.getOptionValue( 'c' );
      }
    }

    ApplicationContext context = new FileSystemXmlApplicationContext( configFile );
    AsyncCache cacheProvider = (AsyncCache) context.getBean( "cacheProvider" );
    cacheProvider.start();
    while ( cacheProvider.isActive() ) {
      try {
        Thread.sleep( 3000 );
      } catch ( InterruptedException e ) {
        log.error( e.getMessage(), e );
      }
    }
    cacheProvider.stop();

  }
}
