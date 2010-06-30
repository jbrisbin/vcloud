mq.on error: {err ->
  println " **** ERROR: ${err.message}"
}

mq.exchange(name: "vcloud.webportal.sessions", durable: true, autoDelete: false) {

  queue name: "session.msgs", routingKey: "vcloud.webportal.sessions.#", {
    consume onmessage: { msg ->
      String body = ""
      if (msg.properties.type == "load") {
        body = msg.bodyAsString
      }
      if (msg.properties.type =~ /attr$/) {
        body = "${msg.properties.headers.id} ${msg.properties.headers.attribute}"
      }
      println "SESSION: ${msg.properties.type} ${msg.properties.replyTo} ${body}"
      return true
    }
  }

}
