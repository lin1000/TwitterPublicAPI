package demo.lin1000.twitter;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class GetTwitterFollower {
    /**
     * Usage: java twitter4j.examples.user.LookupUsers [screen name[,screen name..]]
     *
     * @param args message
     * @throws IOException 
     * @throws TwitterException 
     * @throws ClassNotFoundException 
     * @throws SQLException 
     */
	int rows=0;
    public static void main(String[] args) throws IOException, TwitterException, ClassNotFoundException, SQLException {
    	
    	GetTwitterFollower getTwitterFollower=new GetTwitterFollower();
		double start = System.currentTimeMillis() ; 
		

    	Util.loadConfigProperties();
//    	String sql=" select distinct AUTHOR_NICKNAME from SMA.AUTHOR_VIEW WHERE PERMANENT_COUNTRY='Australia' and PERMANENT_CITY!='Sydney' and PERMANENT_CITY!='Melbourne' and AUTHOR_NICKNAME!='not available' fetch first 118 rows only";
    	//fetch first 300 rows only
//    	String url = "jdbc:db2://10.107.119.236:60000/"+args[0]; //PRO00096
    	
//		String user =args[1];
//		String pwd =args[2]; 
		
//		String UrlFilePath=args[3];
//		String noUrlFilePath=args[4];

		/*
		 * Initialize file writer stream.
		 */		

        
  //      BufferedWriter noUrlBw = getTwitterFollower.iniFileOutStream(noUrlFilePath,"TwitterName" + "," + "TweetsCount");
       
        /*
         * Initialize API configuration.
         */
    	Twitter twitter = getTwitterFollower.getInstance();

//    	Connection conn=getTwitterFollower.getConnection(url, user, pwd);
		
//    	String NamesInput=getTwitterFollower.getExcuteResult(conn, sql);
		String NamesInput="modelpress,kenichiromogi,HikaruIjuin"; 
		
		getTwitterFollower.traversalNamesInput(NamesInput,twitter);
    }
    
    
    
    public Twitter getInstance()
    {
    	ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setDebugEnabled(true)
    	  .setOAuthConsumerKey(Util.getConsumerKey())
    	  .setOAuthConsumerSecret(Util.getConsumerSecret())
    	  .setOAuthAccessToken(Util.getAccessToken())
    	  .setOAuthAccessTokenSecret(Util.getAccessTokenSecret());
    	TwitterFactory tf = new TwitterFactory(cb.build());
    	Twitter twitter = tf.getInstance();
		return twitter;
    }
    
    
    public BufferedWriter iniFileOutStream(String FilePath,String header) throws IOException
    {
		File file = new File(FilePath);
        FileOutputStream out = new FileOutputStream(file);
        OutputStreamWriter osw = new OutputStreamWriter(out, "UTF8");
        BufferedWriter Bw = new BufferedWriter(osw);
        Bw.write(header);
        Bw.newLine();
        return Bw;
    }
    
    public void traversalNamesInput(String NamesInput,Twitter twitter) throws TwitterException, IOException
    {
		if(NamesInput.length()!=0){
			
			
            String[] term=NamesInput.split(",");
            int index=0;
            int test=term.length;    //71426
            int iter=term.length/100;    // 714
            int remainder=term.length%100;  //26
    		
    		for(int i=0;i<iter;i++)
    		{
    			String [] input=new String[100];
    			for(int j=0;j<100;j++)
    			{
    				
    				input[j]=term[index++];
    				System.out.println(input[j]);
    			}
    			
        		search(twitter, input);
        		if(i!=0&&i%100==0)
        		{
        			try {
        				System.out.println("sleep ...");
						Thread.sleep(900000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					};
        		}
    		}
    		
    		String [] inputRemainder=new String[(remainder)];
    		for(int i=0;i<((inputRemainder.length));i++)
    		{
    			inputRemainder[i]=term[index++];
    			System.out.println(inputRemainder[i]);
    		}
    		index--;
    		int RemainLength=inputRemainder.length;
    		if(inputRemainder.length!=0){
    			search(twitter, inputRemainder);
    		}
		}
    }
    
    public String getExcuteResult(Connection conn,String sql) throws SQLException
    {
		Statement stmt = conn.createStatement();
		System.out.println("Start to query data");
		ResultSet rs = stmt.executeQuery(sql);
		System.out.println("Query succeefully");
		System.out.println("--------------------------------------------------------------");
		int count=0;
		String NamesInput = "";
		while(rs.next())
		{
			String name=rs.getString("AUTHOR_NICKNAME");
			System.out.println(name);
			NamesInput+=name+",";
			count++;
		}
		System.out.println("There are "+count+" records in db.");
		System.out.println("--------------------------------------------------------------");
		conn.close();
		return NamesInput;
    }
    
    public Connection getConnection(String url,String user,String pwd) throws ClassNotFoundException, SQLException
    {
    	Class.forName("com.ibm.db2.jcc.DB2Driver");
    	Connection conn = DriverManager.getConnection(url, user, pwd);
		if (!conn.isClosed()) {
			System.out.println("Succeeded connecting to the DB2 !");
			System.out.println("--------------------------------------------------------------");
		}
		return conn;
    	
    }
    
    
	public void search(Twitter twitter,String input[]) throws TwitterException, IOException
	{
		ResponseList<User> users = twitter.lookupUsers(input);


		File file = null;
		FileOutputStream out = null;
		OutputStreamWriter osw = null;
		BufferedWriter bw = null;

	try{
	    for (User oneuser : users)
	    {	
			
	    	int tweetCount=oneuser.getStatusesCount();
	    	String tweetUrl=oneuser.getURL();
	      	String tweetScreenName=oneuser.getScreenName();
		    if(tweetUrl == null)
		    {
		       System.out.println(tweetScreenName+"  "+tweetCount);
		       rows++;
		    }
		    else
		    {
		       System.out.println(tweetScreenName+" @ "+tweetUrl+"   "+tweetCount);
		       rows++;

		    }

			file = new File(tweetScreenName+".followers");
			out = new FileOutputStream(file);
			osw = new OutputStreamWriter(out, "UTF8");
			bw = new BufferedWriter(osw);
			bw.write("handle,screen name,followers count");
			bw.newLine();		

			long cursor = -1;
			PagableResponseList<User> followers;
			do {
				System.out.println("Getting getFollowersList =" + tweetScreenName);
    			 followers = twitter.getFollowersList(tweetScreenName, cursor);
				for (User follower : followers) {
				// TODO: Collect top 10 followers here
					System.out.println(follower.getScreenName()+ " (" + follower.getName() + ") has " + follower.getFollowersCount() + " follower(s)");
					bw.write(follower.getScreenName()+","+follower.getName()+","+follower.getFollowersCount());
					bw.newLine();
				}
			} while ((cursor = followers.getNextCursor()) != 0);
	    }
	}
	catch(Exception e){
		e.printStackTrace();
	}finally{
		bw.close();
		osw.close();
		out.close();
		
	}

	}
	
}
