mq.on error: {err ->
  println " ERROR: ${err.message}"
}

mq {channel ->
  // Delete exchanges
  //channel.exchangeDelete("vcloud.events.mbean")
  //channel.queueDelete("events")
  //channel.queueDelete("events.mbean")
  //channel.queueDelete("events.server.instance")
}

mq.exchange(name: "vcloud.events", type: "direct") {
  queue(name: null, routingKey: "mbean.response") {
    consume onmessage: {msg ->
      println "Response: ${msg.bodyAsString}"
      return true
    }
  }
}

mq.exchange(name: "vcloud.events.mbean") {
  queue(routingKey: "tcserver.server.instance") {
    println "Sending messages to remote mbean..."
    publish body: {msg, out ->
      msg.properties.replyTo = "vcloud.events,mbean.response"
      out.write('{ "mbean": "java.lang:type=Runtime", "attribute": "VmName" }'.bytes)
      out.flush()
    }

    publish body: {msg, out ->
      msg.properties.replyTo = "vcloud.events,mbean.response"
      out.write('{ "mbean": "java.lang:type=Memory", "attribute": "HeapMemoryUsage" }'.bytes)
      out.flush()
    }
  }
}

