# Virtual Private Cloud Utilities

## JMX MBean Invoker

The mbean-invoker module provides a listener inside the Tomcat/tcServer instance
that waits for messages telling it to invoke JMX operations or return the values of
JMX attributes. Designed to be used in conjunction with monitor and management software
to unobtrusively, and with minimal system overhead, manage a vcloud of Tomcat/tcServer
nodes.

#### Dependencies ####

* RabbitMQ AMQP Java client libraries (ver 1.7).
* Jackson JSON Parser/Generator (ver 1.5).

## Cloud-based Session Clustering

This module provides a session Manager and session Store that work in concert
with RabbitMQ to provide session failover and cluster-wide load-balancing without
relying on sticky sessions.

NOTE: I've switched my servers to using SLF4J/Log4J. I blogged about this switch:

http://jbrisbin.wordpress.com/2010/04/20/change-logging-package-to-slf4jlog4j-in-tcservertomcat/

I've created a branch of this that uses the default Tomcat JULI logging. (Note: I deleted that
branch and will re-create it when I get the latest changes I've checked in JULI-fied).

## AMQP Log4J Appender ##

This module provides an appender which publishes Log4J events to your RabbitMQ servers
for the purpose of aggregating log files from multiple sources into a coherent single sink.
To come: a command-line utility to sniff the logging queue