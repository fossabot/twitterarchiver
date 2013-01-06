package twitterarchiver;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

public class UserStorer implements TwitterFeedListener {

  private final DB client;

  public UserStorer() throws UnknownHostException {
    client = MongoClient.connect(new DBAddress(DBAddress.defaultHost(), DBAddress.defaultPort(), "twitter"));
  }

  @Override
  public void messageReceived(TwitterFeedEvent se) {
    try {
      JsonNode user = se.getNode().get("user");
      if (user != null) {
        DBCollection users = client.getCollection("users");
        BasicDBObject o = new BasicDBObject();
        Iterator<Map.Entry<String,JsonNode>> fields = user.fields();
        while(fields.hasNext()) {
          Map.Entry<String, JsonNode> next = fields.next();
          String key = next.getKey();
          if (key.equals("id")) {
            key = "_id";
          }
          o.put(key, next.getValue().asText());
        }
        users.save(o);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void tooSlow() {

  }

  static ByteBuffer $(String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
  }
}
