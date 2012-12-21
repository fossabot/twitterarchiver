package twitterarchiver;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Feb 14, 2010
 * Time: 6:31:02 PM
 */
public class TwitterFeedEvent {
  private JsonNode node;
  private String line;

  public String getLine() {
    return line;
  }

  public JsonNode getNode() {
    return node;
  }

  public TwitterFeedEvent(JsonNode node, String line) {
    this.node = node;
    this.line = line;
  }
}