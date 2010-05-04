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
					 storeId="${instance.id}"
					 operationMode="oneforall"
					 loadTimeout="5"
					 mqHost="localhost"
					 mqPort="5672"
					 mqUser="guest"
					 mqPassword="guest"
					 mqVirtualHost="/"
					 eventsExchange="vcloud.dev.events"
					 eventsQueue="vcloud.dev.events.${instance.id}"
					 sourceEventsQueue="vcloud.dev.source.${instance.id}"
					 sessionEventsExchange="vcloud.dev.sessions"
					 sessionEventsQueuePattern="vcloud.dev.sessions.%s"
					 replicationEventsExchange="vcloud.dev.replication"
					 replicationEventsQueue="vcloud.replication.${instance.id}"/&gt;
  &lt;/Manager&gt;
  &lt;Valve className="com.jbrisbin.vcloud.session.CloudSessionReplicationValve"/&gt;

&lt;/Context&gt;
</code></pre>

The property "instance.id" in this example should be unique throughout the cloud. How you
get a cloud-unique name depends on your setup. I use convention over configuration, so
I concatenate the VM hostname with an instance id that's unique to that node. An
example would be "vm_172_23_10_13.tc1".

#### Operation Modes

Right now, "oneforall" mode is the only really functional mode of operation. It's not as
performant, of course, as having local objects pulled from a Map. I'm working on that.

#### Binding Pattern

In order to load sessions, the store that has the object in its internal Map has to bind
its "sourceEventsQueue" using the pattern defined in sessionEventsQueuePattern. The "%s"
will be replaced by the actual session ID.

#### Note:

The proper (durable) exchanges will be created and bound when the Store is started. The
property "deleteQueuesOnStop" controls whether it should delete the queues for this node
when the Store's stop() method is called. It defaults to true.
