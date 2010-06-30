mq.on error: {err ->
  println " **** ERROR: ${err.message}"
}

mq.exchange(name: "vcloud.classloaders") {

  queue name: "resource.requestor", routingKey: "#", {
    consume ack: true, onmessage: { msg ->
      println "Got ${msg.body.length} bytes for ${msg.properties.type}://${msg.properties.headers.resourceName}"
      return true
    }
  }

  publish routingKey: "com.rabbitmq.client.Connection", body: { msg, out ->
    msg.properties.type = "class"
    msg.properties.replyTo = "resource.requestor"
  }
}