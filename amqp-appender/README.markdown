# AMQP/RabbitMQ Log4J Appender #

Managing log files in the cloud can be a real pain. SSH terminal sessions work okay,
but what if you want to aggregate files from multiple server instances because you're
troubleshooting a problem and you can't reasonably watch 12 individual log files at once?

This is why I wrote this appender. It's pretty simple. There's not many moving parts.
Here's an example configuration:

<pre><code>
  &lt;appender name="cloud" class="com.jbrisbin.vcloud.logging.RabbitMQAppender"&gt;
    &lt;param name="AppenderId" value="${instance.id}"/&gt;
    &lt;param name="Host" value="mq.cloud.mycompany.com"/&gt;
    &lt;param name="User" value="guest"/&gt;
    &lt;param name="Password" value="guest"/&gt;
    &lt;param name="VirtualHost" value="/"/&gt;
    &lt;param name="Exchange" value="vcloud.logging.events"/&gt;
    &lt;layout class="org.apache.log4j.PatternLayout"&gt;
      &lt;param name="ConversionPattern" value="%d %-5p %c{1} %m%n"/&gt;
    &lt;/layout&gt;
  &lt;/appender&gt;

  &lt;category name="com.jbrisbin.vcloud.logging"&gt;
    &lt;level value="DEBUG"/&gt;
    &lt;appender-ref ref="console"/&gt;
  &lt;/category&gt;
  &lt;category name="com.jbrisbin.vcloud"&gt;
    &lt;level value="DEBUG"/&gt;
    &lt;appender-ref ref="cloud"/&gt;
  &lt;/category&gt;
</code></pre>

The import stuff is the "AppenderId", which you need to set so your log aggregator can
distinguish logging events from one server versus another; the RabbitMQ server info
(host, username, password, virtual host, port isn't listed); and the topic exchange to
which your logging events will be published.

### How Events are Published ###

Logging events get published to the configured exchange using a routing key that is a
combination of Level and Category. For example, at DEBUG level, logging for category
com.jbrisbin.vcloud would, in the configuration above, go to a queue named:

<pre><code>DEBUG.com.jbrisbin.vcloud.session.CloudStore</code></pre>

I'm planning on making this configurable. You'd then pass a pattern the appender would
format into the routing key. When I get time...