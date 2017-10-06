package demo.lin1000.twitter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/***
 * args[0] : path to input file
 */

public class CountTweetsParallel{

    int counter = 0;
    
    List<String> readSmallTextFile(String aFileName) throws IOException {
        Path path = Paths.get(aFileName);
        return Files.readAllLines(path, StandardCharsets.UTF_8);
      }

      @SuppressWarnings("IOException")
      List<JsonObject> readLargerTextFile(String aFileName) throws IOException {

          List<JsonObject> inputList = new ArrayList<JsonObject>();

          File inputF = new File(aFileName);
          InputStream inputFS = new FileInputStream(inputF);
          BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
              
          JsonParser jsonParser = new JsonParser();
          inputList = br.lines().parallel().filter(x->x.startsWith("{")).map((x)->{
              try{
                 return  jsonParser.parse(x).getAsJsonObject();
              }catch(com.google.gson.JsonSyntaxException e){
                  System.out.println(e);
                  return null;
              }
          }).collect(Collectors.toList());
          br.close();
          return inputList;
      }

    public static void main(String args[]){
        System.out.println("Count Tweets");

        if(args == null || args.length!=2){
            System.out.println("Usage java demo.lin1000.twitter.CountTweetsParallel <path_to_input_file> <Files|Scanner>");
            System.exit(1);
        }

        String pathToInputFile = args[0];
        String approach = args[1];

        CountTweetsParallel countTweetsParallel =  new CountTweetsParallel();
        List<String> list = null;
        List<JsonObject> listJsonObject = null;

        switch (approach){

            case "Files":                
                try {
                    list = countTweetsParallel.readSmallTextFile(pathToInputFile);
                    System.out.println("Lines : " + list.stream().filter((s)-> s.startsWith("{")).count());

                    List<String> jsonStringList = list.stream().filter((s)-> s.startsWith("{")).collect(Collectors.toList());
                    System.out.println("jsonStringList Lines : " + jsonStringList.size());

                    Gson g = new GsonBuilder().setPrettyPrinting().create(); 

                    JsonObject jsonObj = jsonStringList.stream().map(x-> new JsonParser().parse(x).getAsJsonObject()).max(Comparator.comparing(x->x.get("friends_count").getAsInt())).get();

                    System.out.println(g.toJson(jsonObj));
                    System.out.println("maxFriendsCount="+jsonObj.get("friends_count").getAsInt());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "Parallels":
                
                try {
                    listJsonObject = countTweetsParallel.readLargerTextFile(pathToInputFile);
                    System.out.println("Lines : " + listJsonObject.size());

                    Integer total = listJsonObject.stream().map(x->1).reduce(0,(x,y)->(x+y));
                    System.out.println("Reducer Counted Total : " + total);

                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
   
    }
}