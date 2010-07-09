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
    RabbitMQAsyncCacheProvider cacheProvider = context
        .getBean( RabbitMQAsyncCacheProvider.class );
    while ( cacheProvider.isActive() ) {
      try {
        Thread.sleep( 3000 );
      } catch ( InterruptedException e ) {
        log.error( e.getMessage(), e );
      }
    }

  }
}
