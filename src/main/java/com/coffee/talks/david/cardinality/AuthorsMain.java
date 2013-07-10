package com.coffee.talks.david.cardinality;



import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeSet;

import org.json.JSONObject;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;


/**
 * A simple demo of STREAM-LIB's HyperLogLog:
 * Finding the number of unique distinct authors in a stream of tweets 
 * 
 * @author david
 *
 */
public class AuthorsMain 
{
    //HyperLogLog with m = 2^11 = 2048 registers
    private static int LOG2m = 11;
      
    public static void main( String[] args ) throws IOException
    {                
 
        HyperLogLog hll = new HyperLogLog(LOG2m);
        TreeSet<String> set = new TreeSet<String>();
        
        FileInputStream fileIn = new FileInputStream(args[0]);
        InputStreamReader readerIn = new InputStreamReader(fileIn,"utf-8");
        BufferedReader in = new BufferedReader(readerIn); 
        
        
        int cnt = 0;
        String line;
        while ((line = in.readLine()) != null) {
                                           
                try {
                    JSONObject tweet = new JSONObject(line);
                    if (tweet.has("delete") || tweet.has("limit")) {
                        continue;
                    }
                    JSONObject user = tweet.getJSONObject("user");
                    hll.offer(user.getString("screen_name"));
                    set.add(user.getString("screen_name"));
                    
                    cnt += 1;
                    if (cnt % 10000== 0) {
                        
                        System.out.println("Tweets: " + cnt + ",  Exact authors: " + set.size() + ",  Aprox authors: " + hll.cardinality());
                    }
                    
                    
                } catch (Exception e) {
                        System.out.println("Problem prcessing line:" + line);
                        e.printStackTrace();  
                }
        }
        in.close();
        
        
    }
}
