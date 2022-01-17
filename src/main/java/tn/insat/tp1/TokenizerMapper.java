package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends
        Mapper<LongWritable, Text, Text, DoubleWritable>
{

    /**
     * The `Mapper` function. It receives a line of input from the file,
     * extracts `region name` and `earthquake magnitude` from it, which becomes
     * the output. The output key is `region name` and the output value is
     * `magnitude`.
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {

        String[] line = value.toString().split(",", 12);

        // Ignore invalid lines
      /*  if (line.length != 12) {
            System.out.println("- " + line.length);
            return;
        }*/




        try{
            // The output `key` is the name of the region
            String outputKey = line[3];
            // The output `value` is the magnitude of the earthquake
            double outputValue = Double.parseDouble(line[9]);
    //        System.out.println("ddddd"+outputValue);
            context.write(new Text(outputKey), new DoubleWritable(outputValue));

        } catch(NumberFormatException ex){ // handle your exception
            System.out.println("aaaaa"+ex);

        }
        // Record the output in the Context object
    }
}