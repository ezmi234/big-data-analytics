package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second job
 */
class MapperBigData2 extends Mapper<
                    Text,  // Input key type
                    Text, 		  // Input value type
                    NullWritable,         // Output key type
                    Text> {		  // Output value type
	
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            System.out.println(key.toString()+"_"+value.toString());
    		/* Implement the map method */ 
            context.write(NullWritable.get(), new Text(key.toString()+"_"+value.toString()));

    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
