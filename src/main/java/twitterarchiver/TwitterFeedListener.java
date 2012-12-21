package twitterarchiver;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Feb 14, 2010
 * Time: 6:30:29 PM
 */
public interface TwitterFeedListener {
  public void messageReceived(TwitterFeedEvent se);

  public void tooSlow();
}