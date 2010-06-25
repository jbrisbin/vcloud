mq.on error: {err ->
  println " **** ERROR: ${err.message}"
}

mq.exchange(name: "vcloud.logging.events", durable: true, autoDelete: false) {
  queue name: "vcloud.logging.test", routingKey: "#", {
    consume onmessage: { msg ->
      print "${msg.properties.correlationId} ${msg.envelope.routingKey} ${msg.bodyAsString}"
      return true
    }
  }
}