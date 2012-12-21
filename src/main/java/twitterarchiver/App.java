package twitterarchiver;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Hello world!
 */
public class App {

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

  @Argument
  private static Boolean firehose = false;

  private static class StreamProvider {

    public StreamProvider() {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          synchronized (StreamProvider.this) {
            try {
              stream.close();
              running = false;
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    private boolean running = true;
    private long last = 0;
    private OutputStream stream;

    public synchronized OutputStream getStream() throws IOException {
      if (running) {
        long newlast = (System.currentTimeMillis() / 3600000) * 3600000l; // hour markers
        if (newlast > last || stream == null) {
          if (stream != null) {
            stream.close();
          }
          last = newlast;
          // Ensures that it doesn't write over a previous file if you stop and restart
          stream = new GZIPOutputStream(new FileOutputStream("sample" + System.currentTimeMillis() + ".json.gz"));
        }
        return stream;
      } else {
        return null;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    try {
      Args.parse(App.class, args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      Args.usage(App.class);
      System.exit(1);
    }
    final AtomicLong tweets = new AtomicLong(0);
    final AtomicLong dropped = new AtomicLong(0);
    final AtomicLong last = new AtomicLong(System.currentTimeMillis());
    final ExecutorService es = Executors.newFixedThreadPool(2);
    Properties auth = new Properties();
    auth.load(App.class.getResourceAsStream("/auth.properties"));
    TwitterFeed twitterFeed = new TwitterFeed(auth.getProperty("username"),
            auth.getProperty("password"),
            "https://stream.twitter.com/1/statuses/sample.json", 600000);// 10 minutes
    final StreamProvider jsonStreamProvider = new StreamProvider();
    twitterFeed.addEventListener(new TwitterFeedListener() {
      JsonFactory jf = new MappingJsonFactory();

      // Avoid getting backed up and running out of memory
      Semaphore semaphore = new Semaphore(10000);

      @Override
      public void messageReceived(final TwitterFeedEvent se) {
        try {
          // Skip deletes, etc.
          if (se.getNode().get("text") != null) {
            final OutputStream jsonStream = jsonStreamProvider.getStream();
            if (jsonStream != null) {
              if (semaphore.tryAcquire()) {
                es.submit(new Runnable() {
                  @Override
                  public void run() {
                    try {
                      writeJson(se.getNode(), jsonStream);
                    } catch (Exception e) {
                      e.printStackTrace();
                    } finally {
                      semaphore.release();
                    }
                  }
                });
              } else {
                dropped.incrementAndGet();
              }
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void tooSlow() {
        System.out.println("Too slow!");
      }

      ThreadLocal<SimpleDateFormat> formatter = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          // Fri Dec 21 18:14:35 +0000 2012
          return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        }
      };

      Long getLong(JsonNode node, String name) {
        JsonNode jsonNode = node.get(name);
        if (jsonNode == null) {
          return null;
        } else {
          return jsonNode.asLong();
        }
      }

      private void writeJson(JsonNode s, OutputStream stream) throws IOException {
        long l = tweets.incrementAndGet();
        if (l % 100000 == 0) {
          long now = System.currentTimeMillis();
          long start = last.getAndSet(now);
          System.out.println(100000 * 1000 / (now - start) + " " + tweets + " " + dropped);
        }
        JsonGenerator g = jf.createGenerator(stream);
        g.writeStartObject();
        g.writeStringField(TEXT, s.get("text").textValue());
        g.writeNumberField(ID, getLong(s, "id"));
        JsonNode u = s.get("user");
        g.writeNumberField(USER_ID, u.get("id").longValue());
        try {
          g.writeNumberField(CREATED_AT, formatter.get().parse(s.get("created_at").textValue()).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
        Long inReplyToStatusId = getLong(s, "in_reply_to_status_id");
        if (inReplyToStatusId != null) {
          g.writeNumberField(IN_REPLY_TO_ID, inReplyToStatusId);
        }
        JsonNode retweetedStatus = s.get("retweeted_status");
        if (retweetedStatus != null) {
          g.writeNumberField(RETWEETED_ID, getLong(retweetedStatus, "id"));
        }
        JsonNode entities = s.get("entities");
        if (entities != null && !entities.isNull()) {
          JsonNode userMentionEntities = entities.get("user_mentions");
          if (userMentionEntities != null && userMentionEntities.size() > 0) {
            g.writeArrayFieldStart(USER_MENTION_IDS);
            for (JsonNode userMentionEntity : userMentionEntities) {
              g.writeNumber(getLong(userMentionEntity, "id"));
            }
            g.writeEndArray();
          }
          JsonNode hashtagEntities = entities.get("hashtags");
          if (hashtagEntities != null && hashtagEntities.size() > 0) {
            g.writeArrayFieldStart(HASHTAGS);
            for (JsonNode hashtagEntity : hashtagEntities) {
              g.writeString(hashtagEntity.get("text").textValue());
            }
            g.writeEndArray();
          }
          JsonNode urlEntities = entities.get("urls");
          if (urlEntities != null && urlEntities.size() > 0) {
            g.writeArrayFieldStart(URLS);
            for (JsonNode urlEntity : urlEntities) {
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
              g.writeArrayFieldStart(GEO);
              g.writeNumber(coordinates.get(0).doubleValue());
              g.writeNumber(coordinates.get(1).doubleValue());
              g.writeEndArray();
            }
          }
        }
        if (u.get("verified").asBoolean()) {
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
        synchronized (this) {
          g.flush();
          stream.write('\n');
        }
      }

    });
    twitterFeed.run();
  }
}
