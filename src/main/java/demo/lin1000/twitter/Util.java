package demo.lin1000.twitter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

public class Util {

	// get logger
	static Logger log = Logger.getLogger(Util.class.getName());

	private static int numOfKeys;

	private static ArrayList<String> consumerKey=new ArrayList<String>();
	
	private static ArrayList<String> consumerSecret=new ArrayList<String>();
	
	private static ArrayList<String> accessToken=new ArrayList<String>();
	
	private static ArrayList<String> accessTokenSecret=new ArrayList<String>();
	
	public static void loadConfigProperties()
	{
		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
						
			String resourceName1="twitter4j.properties";
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			InputStream resourceStream = loader.getResourceAsStream(resourceName1);
			log.info(resourceStream.toString());
			properties.load(resourceStream);		

		} catch (IOException e) {
			log.severe("Cannot Find twitter4j.properties");
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		numOfKeys = Integer.valueOf(properties.getProperty("number.of.keys.activate"));

		for (int i=0; i<numOfKeys ;i++){
			log.info("loading twitter api ket sets ");
			consumerKey.add(properties.getProperty("oauth.consumerKey."+(i+1)));
			consumerSecret.add(i,properties.getProperty("oauth.consumerSecret."+(i+1)));
			accessToken.add(i,properties.getProperty("oauth.accessToken."+(i+1)));
			accessTokenSecret.add(i,properties.getProperty("oauth.accessTokenSecret."+(i+1)));

			log.info("consumerKey["+i+"]="+consumerKey.get(i));
			log.info("consumerSecret["+i+"]="+consumerSecret.get(i));
			log.info("accessToken["+i+"]="+accessToken.get(i));
			log.info("accessTokenSecret["+i+"]="+accessTokenSecret.get(i));
		}

	}

	public static int getNumOfKeys(){
		return numOfKeys;
	}
	
	public static String getConsumerKey(int index)
	{
		return consumerKey.get(index);
	}
	public static String getConsumerSecret(int index)
	{
		return consumerSecret.get(index);
	}
	
	public static String getAccessToken(int index)
	{
		return accessToken.get(index);
	}
	
	public static String getAccessTokenSecret(int index)
	{
		return accessTokenSecret.get(index);
	}

}
