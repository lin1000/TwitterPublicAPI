package demo.lin1000.twitter;

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
public class TwitterAPIKeyResourceControl {

    private static final int resNum = 3;
    private static final int secondsToRetryOnAccquire=2; 
    private static final Set<Twitter> freeSet = new HashSet<Twitter>();
    private static final Set<Twitter> lockSet = new HashSet<Twitter>();

    static{
        Util.loadConfigProperties();
        for(int i=0;i < resNum;i++)
            freeSet.add(getTwtterClient(new TwitterAPIKey(Util.getConsumerKey(i),Util.getConsumerSecret(i),Util.getAccessToken(i),Util.getAccessTokenSecret(i))));
        
    }

    
    public static Twitter getTwtterClient(TwitterAPIKey twitterAPIKey)
    {
    	ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setJSONStoreEnabled(true)
    	  .setOAuthConsumerKey(twitterAPIKey.getConsumerKey())
    	  .setOAuthConsumerSecret(twitterAPIKey.getConsumerSecret())
    	  .setOAuthAccessToken(twitterAPIKey.getAccessToken())
    	  .setOAuthAccessTokenSecret(twitterAPIKey.getAccessTokenSecret());
    	TwitterFactory tf = new TwitterFactory(cb.build());
    	Twitter twitter = tf.getInstance();
		return twitter;
    }
        

    public static Twitter getLock() throws Exception{

        Twitter lockObj = null;
        synchronized(TwitterAPIKeyResourceControl.class){
            
            while(lockSet.size()==resNum){
                TimeUnit.SECONDS.sleep(secondsToRetryOnAccquire);
                if(lockSet.size()<resNum){
                    break;
                } 
            }
            //accquire lock
            synchronized(freeSet){
                lockObj = freeSet.iterator().next();
                //remove lock object from freeSet
                freeSet.remove(lockObj);
                //put lock object into lockSet
                lockSet.add(lockObj);
            }
        }
        return lockObj;
    }

    public static void releaseLock(Twitter obj) throws Exception{
        
        if(obj == null) throw new Exception ("lock obj is null, nothing to release");
        synchronized(freeSet){
            if(!lockSet.contains(obj)) throw new Exception("lock obj is not issued by this resource controller");
            lockSet.remove(obj);
            freeSet.add(obj);
        }

    }


}



