package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer second job
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,    // Output key type
                Text> {  // Output value type
    private int threshold = 10;
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
        int countShort = 0;
        int countLong = 0;
        /* Implement the reduce method */
        for (Text intWritable : values) {
            if(Integer.parseInt(intWritable.toString())>threshold){
                countLong++;
            } else{
                countShort++;
            }
        }
        context.write(key, new Text(countLong+","+countShort));
    }

}
