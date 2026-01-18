package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer second job
 */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                NullWritable,    // Output key type
                Text> {  // Output value type
                
                private int globalMax;
                private String globalMeeting;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
                globalMax = -1;
                globalMeeting = null;
        return;
    }
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
        /* Implement the reduce method */

        for (Text text : values) {
            System.out.println(text.toString());
            String meeting = text.toString().split("_")[0];
            int n = Integer.parseInt(text.toString().split("_")[1]);
            if(globalMax == -1 || 
                globalMax < n ||
                globalMax == n && meeting.compareTo(globalMeeting) > 0 
            ){
                globalMax = n;
                globalMeeting = meeting.toString();
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
            if(globalMeeting != null){
                context.write( NullWritable.get(), new Text(globalMeeting));
            }
        return;
    }
}
