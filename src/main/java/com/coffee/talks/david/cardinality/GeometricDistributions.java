package com.coffee.talks.david.cardinality;


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

public class GeometricDistributions {

    public static boolean succesfulCoinToss() {
        return Math.random() > 0.5;
    }
    
    public static String tossUntilSucces() {
        boolean success = false;
        StringBuilder history = new StringBuilder();
        while (!success) {
          success = succesfulCoinToss();
          String symbol = success ? "1" : "O"; 
          history.append(symbol);                 
        }
        return history.toString();
    }
    
    public static int maxTossesUntilSucces(final int n) {
        int max = 0;
        
        List<String> l = new LinkedList<String>();
        
        for(int i = 0; i < n; i++) {
            String tosses = tossUntilSucces();
            l.add(tosses);
            if (tosses.length() > max) {
                max = tosses.length();
            }
        }
//        java.util.Collections.sort(l,Collections.reverseOrder());
//        for (String s:l) {
//            System.out.println(s);
//        }
        
        return max;
    }
    
    public static void main(String[] args) throws FileNotFoundException {
     
        //final int[] elementsArr = {1,2,4,8,16,32,64,128,256,512,1024,2048,};
        final int total = 64;
        final int many = 1000000;
        
        FileOutputStream fileOut = new FileOutputStream("/home/david/proyectos/realtime-agg/src/main/resources/coin-tossing-t16-1M-m2048.txt");
        PrintStream pout = new PrintStream(fileOut);
        
        int elements = 16;
        //for (int elements = 0; elements < 512; elements++) {
            double mean = 0;
            double meanSq = 0;           
           
            
            for (int i=0; i< many; i++) {
            
                double thisMean = 0;
                for (int l=0; l<total; l++)  {
                    int x = maxTossesUntilSucces(elements);
                    thisMean += x; 
            
                }
                thisMean = thisMean / total;
                mean += thisMean;
                meanSq += thisMean*thisMean;
                pout.println(thisMean);
            }
            
            
            double variance = ((meanSq/many) - Math.pow(mean/many,2) ) * (many/(many-1)) ;
            double deviation = Math.sqrt(variance);
            
            System.out.println(elements + "," + mean/many + "," + deviation ); 
            
        //}
//            
        
        
        //}
        pout.close();
    }
}
