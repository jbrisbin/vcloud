= JMX MBean Invoker =
This vcloud component can be installed into your Tomcat 6.0 server as a
Listener. It will consume events on the queue you define and invoke the
given JMX MBean, JSON-serializing the result.

This gives remote management software to unobtrusively, and with minimal
system overhead, monitor the JVMs of processes running in the cloud.

Here's an example snippet from my server.xml file:

<pre><code>&lt;Listener className="com.jbrisbin.vcloud.mbean.CloudInvokerListener"
          mbeanEventsExchange="vcloud.events.mbean"
          mbeanEventsQueue="mbean.events.server.instance"
          mbeanEventsRoutingKey="tcserver.server.instance"/&gt;
</code></pre>