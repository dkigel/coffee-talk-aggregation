package com.coffee.talks.david.cardinality;



import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class HyperLogLogRandomSetsMain {

    private final static long N = 1000000L;
    private final static long REPETITIONS = 50000L;
    
    
    private static HyperLogLog randomPopulatedHyperLogLog(final long cardinality, final int log2n) {
        System.out.println(log2n);
        HyperLogLog h = new HyperLogLog(log2n);
        Set<Integer> set = new HashSet<Integer>();
        
        while(set.size() < N) {
            int x = (int)(Math.random() * Integer.MAX_VALUE);
            set.add(x);
            h.offer(x);
        }
        return h;
    }
    
    public static void main(String[] args) throws FileNotFoundException {
          
      for (int log2n=1  ; log2n<=13; log2n+=2) {  
        
        FileOutputStream fileOut = new FileOutputStream("/home/david/proyectos/realtime-agg/src/main/resources/hll-1M-1k-" + log2n + ".csv");
        PrintStream pout = new PrintStream(fileOut);
        
        for (int i = 0; i < REPETITIONS; i++ ) {
            long estimatedN = randomPopulatedHyperLogLog(N, log2n).cardinality();
            
            System.out.println(i + "," + estimatedN);
            pout.println(i + "," + estimatedN);
        }
        
        pout.close();
        
        
      }
        
    }
    
}
