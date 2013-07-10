package com.coffee.talks.david.tops;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;


/**
 * A simple demo of STREAM-LIB's StreamSummary:
 * Finding the most retweeted authors in a stream of tweets 
 * 
 * @author david
 *
 */
public class TopsMain 
{
    private static int STREAM_SUMMARY_SIZE = 5000;
    
    public static void main( String[] args ) throws IOException
    {
        StreamSummary<String> approxTop = new StreamSummary<String>(STREAM_SUMMARY_SIZE);
                
        FileInputStream fileIn = new FileInputStream(args[0]);
        InputStreamReader readerIn = new InputStreamReader(fileIn,"utf-8");
        BufferedReader in = new BufferedReader(readerIn); 
        
        
        int cnt = 0;
        String line;
        while ((line = in.readLine()) != null) {
            
                cnt += 1;
                if (cnt >0  && cnt % 10000 == 0) {
                    List<Counter<String>> counters = approxTop.topK(5000);
                                        
                    for (Counter<String> counter: counters.subList(0, 10)) {
                        System.out.println(counter);
                    }
                    System.out.println("Minimum value = " + counters.get(counters.size() - 1).getCount());
                    System.out.println();
                }
                try {
                    JSONObject tweet = new JSONObject(line);
                    if (tweet.has("retweeted_status")) {
                          JSONObject retweetedStatus = tweet.getJSONObject("retweeted_status");
                          JSONObject user = retweetedStatus.getJSONObject("user");
                          approxTop.offer(user.getString("screen_name"));
                    }
                } catch (Exception e) {
                        System.out.println("Problem prcessing line:" + cnt);
                        e.printStackTrace();  
                }
        }
        in.close();
    }
}
