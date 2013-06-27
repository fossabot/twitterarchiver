package twitterarchiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;

import static com.yammer.metrics.Metrics.newCounter;

/**
 * Convert twitter JSON to a lightweight, compressed JSON representation.
 */
public class TweetSerializer implements TwitterFeedListener {
  private static final String TEXT = "t";
  private static final String ID = "i";
  private static final String USER_ID = "u";
  private static final String CREATED_AT = "c";
  private static final String IN_REPLY_TO_ID = "s";
  private static final String RETWEETED_ID = "r";
  private static final String USER_MENTION_IDS = "m";
  private static final String HASHTAGS = "h";
  private static final String URLS = "l";
  private static final String MEDIA = "p";
  private static final String GEO = "g";
  private static final String VERIFIED = "v";
  private static final String FOLLOWERS_FRIENDS_FAVS_STATUSES_LISTED = "z";
  private static final String LANG = "n";

  private final StreamProvider jsonStreamProvider;
  private final Counter urls;
  private final Counter tweets;
  private final Counter dropped;
  private final Counter hashtags;
  private final Counter mentions;
  private final Counter media;
  private final Counter retweets;
  private final Counter verified;
  private final Counter geo;
  private final Counter replies;
  private final Counter delay;
  JsonFactory jf;

  public TweetSerializer(StreamProvider jsonStreamProvider) {
    this.jsonStreamProvider = jsonStreamProvider;
    jf = new MappingJsonFactory();
    formatter = new ThreadLocal<SimpleDateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        // Fri Dec 21 18:14:35 +0000 2012
        return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
      }
    };
    tweets = newCounter(TweetSerializer.class, "tweets");
    urls = newCounter(TweetSerializer.class, "urls");
    dropped = newCounter(TweetSerializer.class, "dropped");
    hashtags = newCounter(TweetSerializer.class, "hashtags");
    mentions = newCounter(TweetSerializer.class, "mentions");
    media = newCounter(TweetSerializer.class, "media");
    retweets = newCounter(TweetSerializer.class, "retweets");
    verified = newCounter(TweetSerializer.class, "verified");
    geo = newCounter(TweetSerializer.class, "geo");
    replies = newCounter(TweetSerializer.class, "replies");
    delay = newCounter(TweetSerializer.class, "delay");
  }

  @Override
  public void messageReceived(final TwitterFeedEvent se) {
    try {
      // Skip deletes, etc.
      if (se.getNode().get("text") != null) {
        final OutputStream jsonStream = jsonStreamProvider.getStream();
        if (jsonStream != null) {
          writeJson(se.getNode(), jsonStream);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void tooSlow() {
    dropped.inc();
  }

  ThreadLocal<SimpleDateFormat> formatter;

  private Long getLong(JsonNode node, String name) {
    JsonNode jsonNode = node.get(name);
    if (jsonNode == null) {
      return null;
    } else {
      return jsonNode.asLong();
    }
  }

  long tracker = 0;

  private void writeJson(JsonNode s, OutputStream stream) throws IOException {
    tweets.inc();
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    JsonGenerator g = jf.createGenerator(buffer);
    g.writeStartObject();
    g.writeStringField(TEXT, s.get("text").textValue());
    g.writeNumberField(ID, getLong(s, "id"));
    JsonNode u = s.get("user");
    g.writeNumberField(USER_ID, u.get("id").longValue());
    try {
      long created_at = formatter.get().parse(s.get("created_at").textValue()).getTime();
      g.writeNumberField(CREATED_AT, created_at);
      delay.inc(System.currentTimeMillis() - created_at);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    Long inReplyToStatusId = getLong(s, "in_reply_to_status_id");
    if (inReplyToStatusId != null && inReplyToStatusId != 0) {
      replies.inc();
      g.writeNumberField(IN_REPLY_TO_ID, inReplyToStatusId);
    }
    JsonNode retweetedStatus = s.get("retweeted_status");
    if (retweetedStatus != null) {
      retweets.inc();
      g.writeNumberField(RETWEETED_ID, getLong(retweetedStatus, "id"));
    }
    JsonNode entities = s.get("entities");
    if (entities != null && !entities.isNull()) {
      JsonNode userMentionEntities = entities.get("user_mentions");
      if (userMentionEntities != null && userMentionEntities.size() > 0) {
        g.writeArrayFieldStart(USER_MENTION_IDS);
        for (JsonNode userMentionEntity : userMentionEntities) {
          mentions.inc();
          g.writeNumber(getLong(userMentionEntity, "id"));
        }
        g.writeEndArray();
      }
      JsonNode hashtagEntities = entities.get("hashtags");
      if (hashtagEntities != null && hashtagEntities.size() > 0) {
        g.writeArrayFieldStart(HASHTAGS);
        for (JsonNode hashtagEntity : hashtagEntities) {
          hashtags.inc();
          g.writeString(hashtagEntity.get("text").textValue());
        }
        g.writeEndArray();
      }
      JsonNode urlEntities = entities.get("urls");
      if (urlEntities != null && urlEntities.size() > 0) {
        g.writeArrayFieldStart(URLS);
        for (JsonNode urlEntity : urlEntities) {
          urls.inc();
          JsonNode expandedURL = urlEntity.get("expanded_url");
          if (expandedURL == null) {
            g.writeString(urlEntity.get("url").textValue());
          } else {
            g.writeString(expandedURL.textValue());
          }
        }
        g.writeEndArray();
      }
      JsonNode mediaEntities = entities.get("media");
      if (mediaEntities != null && mediaEntities.size() > 0) {
        g.writeArrayFieldStart(MEDIA);
        for (JsonNode mediaEntity : mediaEntities) {
          media.inc();
          g.writeString(mediaEntity.get("media_url").textValue());
        }
        g.writeEndArray();
      }
    }
    JsonNode geoLocation = s.get("geo");
    if (geoLocation != null && !geoLocation.isNull()) {
      JsonNode coordinates = s.get("coordinates");
      if (coordinates != null && !coordinates.isNull()) {
        coordinates = coordinates.get("coordinates");
        if (coordinates != null && !coordinates.isNull()) {
          geo.inc();
          g.writeArrayFieldStart(GEO);
          g.writeNumber(coordinates.get(0).doubleValue());
          g.writeNumber(coordinates.get(1).doubleValue());
          g.writeEndArray();
        }
      }
    }
    if (u.get("verified").asBoolean()) {
      verified.inc();
      g.writeBooleanField(VERIFIED, true);
    }
    g.writeArrayFieldStart(FOLLOWERS_FRIENDS_FAVS_STATUSES_LISTED);
    g.writeNumber(u.get("followers_count").longValue());
    g.writeNumber(u.get("friends_count").longValue());
    g.writeNumber(u.get("favourites_count").longValue());
    g.writeNumber(u.get("statuses_count").longValue());
    g.writeNumber(u.get("listed_count").longValue());
    g.writeEndArray();
    g.writeStringField(LANG, u.get("lang").textValue());
    g.writeEndObject();
    g.flush();
    buffer.writeTo(stream);
    stream.write('\n');
  }

}
