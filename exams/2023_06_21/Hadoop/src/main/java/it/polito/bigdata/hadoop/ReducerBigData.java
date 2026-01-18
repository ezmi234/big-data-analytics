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
        
        private String maxMeeting;
        private int localMax;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
                    localMax = -1;
                    maxMeeting = null;
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

            if(localMax == -1 || 
                localMax < count ||
                localMax == count && key.toString().compareTo(maxMeeting) > 0 
            ){
                localMax = count;
                maxMeeting = key.toString();
            }
            
        }
    
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
                if(maxMeeting != null){
                    context.write(new Text(maxMeeting), new IntWritable(localMax));
                }
            return;
        }

}
