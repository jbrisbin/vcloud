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
           operationMode="replicated"
           mqHost="${mq.host}"
           mqPort="${mq.port}"
           mqUser="${mq.user}"
           mqPassword="${mq.password}"
           mqVirtualHost="${mq.virtualhost}"
           eventsExchange="vcloud.events.session"
           eventsQueue="vcloud.events.${instance.id}"
           sourceEventsQueue="vcloud.source.${instance.id}"
           replicationEventsExchange="vcloud.events.replication"
           replicationEventsQueue="vcloud.replication.${instance.id}"
           deleteQueuesOnStop="true"/&gt;
  &lt;/Manager&gt;
  &lt;Valve className="com.jbrisbin.vcloud.session.CloudSessionReplicationValve"/&gt;

&lt;/Context&gt;
</code></pre>

The property "instance.id" in this example should be unique throughout the cloud. How you
get a cloud-unique name depends on your setup. I use convention over configuration, so
I concatenate the VM hostname with an instance id that's unique to that node. An
example would be "vm_172_23_10_13.tc1".

#### Operation Modes

The session manager is now configurable between two modes of operation: "oneforall"
and "replicated". "replicated" is the default mode of operation and, for performance reasons,
replicates the session to everyone; no matter what server your user lands on, their session
will be there. The store keeps track of MD5 hashes and will only replicate a session if it
sees changes in the serialized object. However, this could be quite often if the session is
accessed in the webpp, thus updating the "lastAccessed" time. I'm looking for a good Java-based
binary diff utility I can use to cut down on the size of the messages. That should increase
throughput and performance a fair bit.

"oneforall" mode is still a bit rough around the edges. I'm working on making it more robust,
but at the moment, I've noticed a fair number of annoying issues using more than two servers behind
an Apache proxy. I don't know yet what makes it unstable in this configuration, but I've got
a couple fixes in mind that I'll implement as soon as I can.

#### Note:

The proper (durable) exchanges will be created and bound when the Store is started. The
property "deleteQueuesOnStop" controls whether it should delete the queues for this node
when the Store's stop() method is called. It defaults to true.
