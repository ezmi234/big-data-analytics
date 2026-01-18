package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        IntWritable, // Input value type
        Text, // Output key type
        NullWritable> { // Output value type
            private ArrayList<String> listOfOS = new ArrayList<String>();
            private int maxN = -1; 

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
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
                count+= intWritable.get();
            }

            if(count>maxN){
                maxN = count;
                listOfOS.clear();
                listOfOS.add(key.toString());
            } else if(count==maxN){
                listOfOS.add(key.toString());
            }
        }
    
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            listOfOS.sort(String::compareTo);
            context.write(new Text(listOfOS.get(0)), null);
            return;
        }

}
