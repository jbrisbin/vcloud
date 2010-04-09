# vCloud Utilities

NOTE: The only part of the vCloud Utilities yet finished is the RabbitMQ-based
session replication for Apache Tomcat 6.0.

## Non-multicast Session Clustering

This module provides a session Manager and session Store that work in concert
with RabbitMQ to provide session failover and cluster-wide load-balancing without
relying on sticky sessions.

### Dependencies:

* RabbitMQ Java client
* vCloud Session Manager

### Installation:

1. Copy the `vcloud-session-manager-1.0.jar` to `$CATALINA_BASE/lib`.
2. Copy RabbitMQ client jar to `$CATALINA_BASE/lib`.

In either `META-INF/context.xml` or `$CATALINA_BASE/conf/Catalina/localhost/myapp.xml`
configure the Manager and Store:

<pre><code>&lt;Context path="/myapp" distributable="true"&gt;
	&lt;Manager className="com.jbrisbin.vcloud.session.CloudManager"
					 maxInactiveInterval="900"&gt;
		&lt;Store className="com.jbrisbin.vcloud.session.CloudStore"
					 mqHost="mq.cloud.mycompany.com"
					 mqPort="5672"
					 mqUser="cloud"
					 mqPassword="mypass"
					 mqVirtualHost="/"
					 storeId="${store.id}"
					 eventsExchange="vcloud.session.events"
					 eventsQueue="vcloud.session.${store.id}"
					 replicationEventsExchange="vcloud.replication.events"
					 replicationEventsQueue="vcloud.replication.${store.id}"
					 replicationEventsRoutingKey="vcloud.session.replication"
					 sourceEventsExchange="vcloud.source.events"
					 sourceEventsQueue="vcloud.source.${store.id}"
					 sourceEventsRoutingPrefix="vcloud.source."
					 loadTimeout="15"/&gt;
	&lt;/Manager&gt;
&lt;/Context&gt;
</code></pre>