package demo.lin1000.twitter;

import java.util.stream.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.lang.annotation.Documented;

/***
 * args[0] : path to input file
 */

public class CountTweets{

    int counter = 0;
    
    @SuppressWarnings("IOException")
     List<String> readSmallTextFile( String aFileName) throws IOException {
        Path path = Paths.get(aFileName);
        return Files.readAllLines(path, StandardCharsets.UTF_8);
      }

    
    @SuppressWarnings("IOException")
    void readLargerTextFile(String aFileName) throws IOException {
        Path path = Paths.get(aFileName);
        try (Scanner scanner =  new Scanner(path, StandardCharsets.UTF_8.name()) ){
            while (scanner.hasNextLine()){
            //process each line in some way
                scanner.nextLine();
                counter++;
            }      
         } 

         System.out.println("Lines : " + counter);
    }

    
    public static void main(String args[]){
        System.out.println("Count Tweets");

        if(args == null || args.length!=2){
            System.out.println("Usage java demo.lin1000.twitter.CountTweets <path_to_input_file> <Files|Scanner>");
            System.exit(1);
        }

        String pathToInputFile = args[0];
        String approach = args[1];

        CountTweets countTweets =  new CountTweets();
        List<String> inputTextFile = null;

        switch (approach){

            case "Files":                
                try {
                    inputTextFile = countTweets.readSmallTextFile(pathToInputFile);
                    System.out.println("Lines : " + inputTextFile.size());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                break;
            case "Scanner":
                
                try {
                    countTweets.readLargerTextFile(pathToInputFile);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                break;
            default:
              assert false : approach;
              //throw new AssertionError(approach);
        }
   
    }
}