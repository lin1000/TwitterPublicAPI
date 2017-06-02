package demo.lin1000.twitter;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.*;
import twitter4j.*;
import java.util.Iterator;

public class GetTwitterFollower {

	// get logger
	static Logger log = Logger.getLogger(GetTwitterFollower.class.getName());

	public static class TwitterHandleThread implements Callable<Integer> {
		String handle;

		public TwitterHandleThread(String handle) {
			this.handle = handle;
		}

		@Override
		public Integer call() throws Exception {

			Twitter lockobj = TwitterAPIKeyResourceControl.getLock();

			log.info("handle="+handle+ " lockobj="+lockobj);

			search(lockobj, handle);
			
			TwitterAPIKeyResourceControl.releaseLock(lockobj);

			return 1;
		}

		public void search(Twitter twitter,String input) throws TwitterException, IOException
		{
			boolean isError = false;

			ResponseList<User> users = twitter.lookupUsers(input);

			File file = null;
			FileOutputStream out = null;
			OutputStreamWriter osw = null;
			BufferedWriter bw = null;
			File fileRaw = null;
			FileOutputStream outRaw = null;
			OutputStreamWriter oswRaw = null;
			BufferedWriter bwRaw = null;

			try{
				for (User oneuser : users)
				{	
					
					int tweetCount=oneuser.getStatusesCount();
					String tweetUrl=oneuser.getURL();
					String tweetScreenName=oneuser.getScreenName();

					file = new File(tweetScreenName+".followers.csv");
					out = new FileOutputStream(file);
					osw = new OutputStreamWriter(out, "UTF8");
					bw = new BufferedWriter(osw);
					bw.write("\"id\",\"handle\",\"screenname\",\"followerscount\",\"statuscount\"");
					bw.newLine();		

					fileRaw = new File(tweetScreenName+".followers.json");
					outRaw = new FileOutputStream(fileRaw);
					oswRaw = new OutputStreamWriter(outRaw, "UTF8");
					bwRaw = new BufferedWriter(oswRaw);

					long cursor = -1;
					PagableResponseList<User> followers = null;

					do {
						try{
							log.info("Getting getFollowersList =" + tweetScreenName);
							followers = twitter.getFollowersList(tweetScreenName, cursor,200);

							log.info("Handle:"+ handle+" getRateLimitStatus().getLimit()="+ followers.getRateLimitStatus().getLimit());
							log.info("Handle:"+ handle+" getRateLimitStatus().getRemaining()="+ followers.getRateLimitStatus().getRemaining());
							log.info("Handle:"+ handle+" getRateLimitStatus().getResetTimeInSeconds()="+ followers.getRateLimitStatus().getResetTimeInSeconds());
							log.info("Handle:"+ handle+" getRateLimitStatus().getSecondsUntilReset()="+ followers.getRateLimitStatus().getSecondsUntilReset());	

							if(followers.getRateLimitStatus().getRemaining()>0) {
								// Twiiter API Key Strategy 
								// Strategy option 1: wait
								log.info("==============================Strategy 1 wait until reset====================================");
								waitForReset(twitter);
								followers = twitter.getFollowersList(tweetScreenName, cursor,200);
								//Strategy option 2: switch api key and wait if necessary
								//log.info("Strategy 2 switch api key and wait if necessary");
								//TwitterAPIKeyResourceControl.releaseLock(twitter);
								//twitter = TwitterAPIKeyResourceControl.getLock();
								//log.info("getRateLimitStatus().getLimit()="+ twitter.getRateLimitStatus().getLimit());
								//log.info("getRateLimitStatus().getRemaining()="+ twitter.getRateLimitStatus().getRemaining());
								//log.info("getRateLimitStatus().getResetTimeInSeconds()="+ twitter.getRateLimitStatus().getResetTimeInSeconds());
								//log.info("getRateLimitStatus().getSecondsUntilReset()="+ twitter.getRateLimitStatus().getSecondsUntilReset());
								//Thread.sleep(followers.getRateLimitStatus().getSecondsUntilReset()*1000 + 10000);

							}
						
							for (User follower : followers) {
								//Collect Top N Followers
								String rawJson = TwitterObjectFactory.getRawJSON(follower);
								String row = "\""+ follower.getId()+ ",\"" + follower.getScreenName()+",\""+follower.getName()+",\""+follower.getFollowersCount() + ",\""+follower.getStatusesCount() +"\"";
								bw.write(row);
								bw.newLine();
								bwRaw.write(rawJson);
								bwRaw.newLine();
								bw.flush();
								bwRaw.flush();
							}
						}
						catch(TwitterException e){
							//e.printStackTrace();
							log.warning("Handling Rate limit exceeded Exception");
							waitForReset(twitter);
							followers = twitter.getFollowersList(tweetScreenName, cursor,200);
							continue;
						}
					} while ((cursor = followers.getNextCursor()) != 0);
				}
			}
			catch(Exception e){
				e.printStackTrace();
				bw.write("Not Complete because unidentified exception");
				bwRaw.write("Not Complete because  unidentified exception");
				bw.flush();
				bwRaw.flush();
				isError=true;
			}finally{
				bw.close();
				osw.close();
				out.close();
				bwRaw.close();
				oswRaw.close();
				outRaw.close();	
			}

		log.info("Handle:"+ handle+" Get Twitter Followers isError="+isError);

	}
	
	public void waitForReset(Twitter twitter){
		try{
			log.info("Handle:"+ handle+ ", twitter.getRateLimitStatus(/followers/list)="+twitter.getRateLimitStatus().get("/followers/list"));
			if(twitter.getRateLimitStatus().get("/followers/list").getRemaining()>0){
				//do nothing , pass
			}else{
				log.info("Waiting for " + twitter.getRateLimitStatus().get("/followers/list").getSecondsUntilReset() + " seconds.");
				TimeUnit.SECONDS.sleep(twitter.getRateLimitStatus().get("/followers/list").getSecondsUntilReset()+10);
			}
		}catch(Exception e){
			e.printStackTrace();
			//force to wait for 15 minutes
			try{
				TimeUnit.MINUTES.sleep(15);
			}catch(InterruptedException e1){
				e1.printStackTrace();
				log.severe("Force exit with errors");
				System.exit(1);
			}
		}
	}
			
	}



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

//    	Connection conn=getTwitterFollower.getConnection(url, user, pwd);
		
//    	String NamesInput=getTwitterFollower.getExcuteResult(conn, sql);
		String NamesInput="modelpress,kenichiromogi,HikaruIjuin"; 
		
		getTwitterFollower.traversalNamesInput(NamesInput);
    }
    
    

    
    public void traversalNamesInput(String NamesInput) throws TwitterException, IOException
    {

		ExecutorService executor = Executors.newFixedThreadPool(3);
		ArrayList<Future<Integer>> futures = new ArrayList<Future<Integer>>(3); 

		if(NamesInput.length()!=0){
			
            String[] term=NamesInput.split(",");

			for (int i=0 ; i< term.length;i++){

				TwitterHandleThread twitterHandleThread = new TwitterHandleThread(term[i]); 
				futures.add(i, executor.submit(twitterHandleThread));

			}

				// //Verify wether callable tasks are completed
				// Integer result = null;
				// try{
				// 	result = futures..get();
				// }catch(ExecutionException e){
				// 	e.printStackTrace();
				// }catch(InterruptedException e){
				// 	e.printStackTrace();
				// }

				// log.info("future done? " + future.isDone());
				// log.info("result: " + result);

				try {
					log.info("attempt to shutdown executor");
					executor.shutdown();
					executor.awaitTermination(5, TimeUnit.HOURS);
				}
				catch (InterruptedException e) {
					log.severe("tasks interrupted");
				}
				finally {
					if (!executor.isTerminated()) {
						log.severe("cancel non-finished tasks");
					}
					executor.shutdownNow();
					log.info("shutdown finished");
				}
						
            // int index=0;
            // int test=term.length;    //71426
            // int iter=term.length/100;    // 714
            // int remainder=term.length%100;  //26
    		
    		// for(int i=0;i<iter;i++)
    		// {
    		// 	String [] input=new String[1];
    		// 	for(int j=0;j<1;j++)
    		// 	{
    				
    		// 		input[j]=term[index++];
    		// 		System.out.println(input[j]);
    		// 	}
    			
        	// 	search(twitter, input);
        	// 	if(i!=0&&i%100==0)
        	// 	{
        	// 		try {
        	// 			System.out.println("sleep ...");
			// 			Thread.sleep(900000);
			// 		} catch (InterruptedException e) {
			// 			e.printStackTrace();
			// 		};
        	// 	}
    		// }
    		
    		// String [] inputRemainder=new String[(remainder)];
    		// for(int i=0;i<((inputRemainder.length));i++)
    		// {
    		// 	inputRemainder[i]=term[index++];
    		// 	System.out.println(inputRemainder[i]);
    		// }
    		// index--;
    		// int RemainLength=inputRemainder.length;
    		// if(inputRemainder.length!=0){
    		// 	search(twitter, inputRemainder);
    		// }
		}
    }
    

}
