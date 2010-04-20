# vCloud Utilities

## JMX MBean Invoker
The mbean-invoker module provides a listener inside the Tomcat/tcServer instance
that waits for messages telling it to invoke JMX operations or return the values of
JMX attributes. Designed to be used in conjunction with monitor and management software
to unobtrusively, and with minimal system overhead, manage a vcloud of Tomcat/tcServer
nodes.

#### Dependencies ####
* RabbitMQ AMQP Java client libraries (ver 1.7).
* Jackson JSON Parser/Generator (ver 1.5).

## vCloud Session Clustering
This module provides a session Manager and session Store that work in concert
with RabbitMQ to provide session failover and cluster-wide load-balancing without
relying on sticky sessions.

### Dependencies:

* RabbitMQ Java client
* commons-io

### Installation:

1. Copy the vcloud-session-manager-1.0.jar to $CATALINA_BASE/lib.
2. Copy RabbitMQ client jar (1.7.2 or later) to $CATALINA_BASE/lib.
3. Copy commons-io.jar (1.2 or later) to $CATALINA_BASE/lib.

In either the webapp META-INF/context.xml or $CATALINA_BASE/conf/Catalina/localhost/myapp.xml
configure the Manager and Store:

<pre><code>&lt;Context&gt;

  &lt;Manager className="com.jbrisbin.vcloud.session.CloudManager"&gt;
    &lt;Store className="com.jbrisbin.vcloud.session.CloudStore"
           storeId="${instance.id}"
           operationMode="replicated"
           mqHost="${mq.host}"
           mqPort="${mq.port}"
           mqUser="${mq.user}"
           mqPassword="${mq.password}"
           mqVirtualHost="${mq.virtualhost}"
           eventsExchange="vcloud.session.events"
           eventsQueue="vcloud.events.${instance.id}"
           sourceEventsExchange="vcloud.source.events"
           sourceEventsQueue="vcloud.source.${instance.id}"
           replicationEventsExchange="vcloud.replication.events"
           replicationEventsQueue="vcloud.replication.${instance.id}"
           deleteQueuesOnStop="true"/&gt;
  &lt;/Manager&gt;
  &lt;Valve className="com.jbrisbin.vcloud.session.CloudSessionReplicationValve"/&gt;

&lt;/Context&gt;
</code></pre>

The property "instance.id" in this example should be unique throughout the cloud. How you
get a a cloud-unique name depends on your setup. I use convention over configuration, so
I concatenate the external IP address with an instance id that's unique to that machine.

### NEW:

The session manager is now configurable between two modes of operation: "allforone"
and "replicated". "allforone" (maybe I should rename it "oneforall"...) is the default mode
and has the behaviour mentioned in my blog post. One session exists within the cloud and it
gets moved around from server to server. The mode "replicated" is different in that it always
replicates the session to everyone. No matter what server your user lands on, their session
will be there. Since its not really desireable to blindly replicate all sessions, all the time,
I'm working on a third mode of operation that keeps track of MD5 hashes and will only replicate
a session if it sees changes in the serialized object.

#### Note:

The proper (durable) exchanges will be created and bound when the Store is started. The
property "deleteQueuesOnStop" controls whether it should delete the queues for this node
when the Store's stop() method is called. It defaults to true.

Replication doesn't work yet. There are some logistical hurdles to jump before I have a
solid failover system in place.
