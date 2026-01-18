package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        IntWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type
        
        private int localMax;
        private String maxCity;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            localMax = 0;
            maxCity = "";
            return;
        }
        
        @Override
        protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
            
            /* Implement the reduce method */
            int count = 0;
            for (IntWritable intWritable : values) {
                count += intWritable.get();
            }

            if(count > localMax || 
            (count == localMax && 
            maxCity.compareTo(key.toString()) >= 0)){
                localMax = count;
                maxCity = key.toString();
            }
            
        }
    
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if(localMax != 0){
                context.write(new Text(maxCity), new IntWritable(localMax));
            }
            return;
        }

}
