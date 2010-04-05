package com.jbrisbin.vcloud.session;

/**
 * Created by IntelliJ IDEA.
 * User: jbrisbin
 * Date: Apr 3, 2010
 * Time: 9:21:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class CloudSessionMessage {

  private String type;
  private String source;
  private String id;
  private byte[] body;
  private String replyToExchange;
  private String replyToRoutingKey;

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }

  public String getSource() {
    return source;
  }

  public void setSource( String source ) {
    this.source = source;
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public byte[] getBody() {
    return body;
  }

  public void setBody( byte[] body ) {
    this.body = body;
  }

  public String getReplyToExchange() {
    return replyToExchange;
  }

  public void setReplyToExchange( String replyToExchange ) {
    this.replyToExchange = replyToExchange;
  }

  public String getReplyToRoutingKey() {
    return replyToRoutingKey;
  }

  public void setReplyToRoutingKey( String replyToRoutingKey ) {
    this.replyToRoutingKey = replyToRoutingKey;
  }
}
