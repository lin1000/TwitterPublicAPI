package demo.lin1000.twitter;

public class TwitterAPIKey {

	private String consumerKey = null;
	private String consumerSecret=null;
	private String accessToken=null;
	private String accessTokenSecret=null;
	
    public TwitterAPIKey(String consumerKey,String consumerSecret,String accessToken, String accessTokenSecret){
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;     
    }

    public String getConsumerKey(){return consumerKey;}
    public String getConsumerSecret(){return consumerSecret;}
    public String getAccessToken(){return accessToken;}
    public String getAccessTokenSecret(){return accessTokenSecret;}

}
