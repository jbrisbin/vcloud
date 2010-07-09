## Cloud-based Session Clustering

This module provides a session Manager and session Store that work in concert
with RabbitMQ to provide session failover and cluster-wide load-balancing without
relying on sticky sessions.

### Dependencies:

* RabbitMQ Java client (1.7.2 or later)
* commons-io (1.2 or later)
* SLF4J

### Installation:

1. Copy the vcloud-session-manager-1.x.x.jar to $CATALINA_BASE/lib.
2. Copy RabbitMQ client jar (1.7.2 or later) to $CATALINA_BASE/lib.
3. Copy commons-io.jar (1.2 or later) to $CATALINA_BASE/lib.

In either the webapp META-INF/context.xml or $CATALINA_BASE/conf/Catalina/localhost/myapp.xml
configure the Manager and Store:

<pre><code>&lt;Context&gt;

  &lt;Manager className="com.jbrisbin.vcloud.session.CloudManager"&gt;
		&lt;Store className="com.jbrisbin.vcloud.session.CloudStore"
					 storeId="${instance.replyTo}"
					 operationMode="oneforall"
           maxMqHandlers="5"
           maxRetries="3"
					 loadTimeout="5"
					 mqHost="localhost"
					 mqPort="5672"
					 mqUser="guest"
					 mqPassword="guest"
					 mqVirtualHost="/"
					 eventsExchange="vcloud.dev.events"
					 eventsQueue="vcloud.dev.events.${instance.replyTo}"
					 sourceEventsQueue="vcloud.dev.source.${instance.replyTo}"
					 sessionEventsExchange="vcloud.dev.sessions"
					 sessionEventsQueuePattern="vcloud.dev.sessions.%s"
					 replicationEventsExchange="vcloud.dev.replication"
					 replicationEventsQueue="vcloud.replication.${instance.replyTo}"/&gt;
  &lt;/Manager&gt;

&lt;/Context&gt;
</code></pre>

#### NEW!

Newly added: maxRetries. This is the maximum number of times to try and load the session from
the RabbitMQ servers. I'm still not 100% clear on why trying it a couple times works versus just
a longer timeout value, but the former works and the latter does not.

"maxMqHandlers" is the number of handlers to start when the store fires up. There's a balance
in your setup between an appropriate number of handlers to consume all the events versus lowering
total memory consumption. You'll have to experiment with this setting and tweak to taste.

#### Setup

The property "instance.replyTo" in this example should be unique throughout the cloud. How you
get a cloud-unique name depends on your setup. I use convention over configuration, so
I concatenate the VM hostname with an instance replyTo that's unique to that node. An
example would be "vm_172_23_10_13.tc1".

#### Operation Modes

Right now, "oneforall" mode is the only really functional mode of operation. It's not as
performant, of course, as having local objects pulled from a Map. It's a trade-off.

#### Binding Pattern

In order to load sessions, the store that has the object in its internal Map has to bind
its "sourceEventsQueue" using the pattern defined in sessionEventsQueuePattern. The "%s"
will be replaced by the actual session ID.

#### Note:

The proper (durable) exchanges will be created and bound when the Store is started. The
property "deleteQueuesOnStop" controls whether it should delete the queues for this node
when the Store's stop() method is called. It defaults to true.
