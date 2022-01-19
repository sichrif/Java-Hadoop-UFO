package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class BookMapper  extends
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
        Text word = new Text();
        DoubleWritable one = new DoubleWritable(1);

        String[] line = value.toString().split(",", 12);
        try{
            if(line.length>3){

                // The output `key` is the name of the region
                String outputKey = line[7];
                // The output `value` is the magnitude of the earthquake
                double outputValue = Double.parseDouble(line[7]);
                ////////////////
                StringTokenizer itr = new StringTokenizer(outputKey);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
                ////////////
                //   context.write(new Text(outputKey), new DoubleWritable(outputValue));
            }
        } catch(NumberFormatException ex){ // handle your exception
            System.out.println("aaaaa"+ex);

        }
    }
}