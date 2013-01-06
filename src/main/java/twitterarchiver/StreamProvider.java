package twitterarchiver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Returns the current filename we should use.
 */
public class StreamProvider {

  private String prefix;
  private String filename;

  public StreamProvider(String prefix) {
    this.prefix = prefix;
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

  public synchronized String getFilename() {
    return filename;
  }

  public synchronized OutputStream getStream() throws IOException {
    if (running) {
      long newlast = (System.currentTimeMillis() / 3600000) * 3600000l; // hour markers
      if (newlast > last || stream == null) {
        if (stream != null) {
          stream.close();
        }
        last = newlast;
        // Ensures that it doesn't write over a previous file if you stop and restart
        filename = prefix + System.currentTimeMillis() + ".json.gz";
        stream = new GZIPOutputStream(new FileOutputStream(filename));
      }
      return stream;
    } else {
      return null;
    }
  }
}