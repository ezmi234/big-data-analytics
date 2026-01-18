package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second job
 */
class MapperBigData2 extends Mapper<
                    LongWritable,  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {		  // Output value type
	
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
