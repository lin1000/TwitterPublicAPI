package demo.lin1000.twitter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Util {

	private static String consumerKey=null;
	
	private static String consumerSecret=null;
	
	private static String accessToken=null;
	
	private static String accessTokenSecret=null;
	
//	private static String dbUser=null;
	
//	private static String dbPwd=null;
	
	public static void loadConfigProperties()
	{
		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			// String resourceName="target/classes/twitter4j.properties";
			// String protertyFilePath = (resourceName);
			// inputStream = new FileInputStream(protertyFilePath);
			// properties.load(inputStream);			
			
			String resourceName1="twitter4j.properties";
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			InputStream resourceStream = loader.getResourceAsStream(resourceName1);
			System.out.println(resourceStream);
			properties.load(resourceStream);		

		} catch (IOException e) {
			System.err.println("Cannot Find twitter4j.properties");
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		consumerKey = properties.getProperty("oauth.consumerKey");
		consumerSecret = properties.getProperty("oauth.consumerSecret");
		accessToken = properties.getProperty("oauth.accessToken");
		accessTokenSecret = properties.getProperty("oauth.accessTokenSecret");

		System.out.println("consumerKey="+consumerKey);

	}
	
//	public static String getDBUser()
//	{
//		return dbUser;
//	}
//	public static String getDBPwd()
//	{
//		return dbPwd;
//	}
	public static String getConsumerKey()
	{
		return consumerKey;
	}
	public static String getConsumerSecret()
	{
		return consumerSecret;
	}
	
	public static String getAccessToken()
	{
		return accessToken;
	}
	
	public static String getAccessTokenSecret()
	{
		return accessTokenSecret;
	}

}
