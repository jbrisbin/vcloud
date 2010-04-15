mq.on error: {err ->
  println " ERROR: ${err.message}"
}

mq {channel ->
  // Delete exchanges
}

def members = []

mq.exchange(name: "vcloud.events", type: "topic") {
  queue(name: null, routingKey: "#") {
    consume onmessage: {msg ->
      def key = msg.envelope.routingKey
      def msgBody = msg.bodyAsString
      def source = msg.envelope.routingKey[msgBody.length() + 1..key.length() - 1]
      println "Received ${msgBody} event from ${source}"
      if ( msgBody == "start" ) {
        members << source
      } else if ( msgBody == "stop" ) {
        members.remove(source)
      }

      return true
    }
  }
}

/*
mq.exchange(name: "vcloud.events.mbean") {
  queue(routingKey: "tcserver.server.instance") {
    println "Sending messages to remote mbean..."
    publish body: {msg, out ->
      msg.properties.replyTo = "mbean.response"
      out.write('{ "mbean": "java.lang:type=Runtime", "attribute": "VmName" }'.bytes)
      out.flush()
    }

    publish body: {msg, out ->
      msg.properties.replyTo = "mbean.response"
      out.write('{ "mbean": "java.lang:type=Memory", "attribute": "HeapMemoryUsage" }'.bytes)
      out.flush()
    }
  }
}
*/

