package tn.insat.tp1;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
        extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    private DoubleWritable result = new DoubleWritable();

    /**
     * The `Reducer` function. Iterates through all earthquake magnitudes for a
     * region to find the maximum value. The output key is the `region name` and
     * the value is the `maximum magnitude` for that region.
     * @param key - Input key - Name of the region
     * @param values - Input Value - Iterator over quake magnitudes for region
     * @param context - Used for collecting output
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Standard algorithm for finding the max value
        double maxMagnitude = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
            sum += value.get();
            System.out.println("--> Sum = "+sum);

            //  maxMagnitude = Math.max(maxMagnitude, value.get());
           // context.write(key, new DoubleWritable( value.get()));

        }
        System.out.println("--> Sum = "+sum);
        result.set(sum);

        try{
            System.out.println("firstttttttt");
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            DB database = mongoClient.getDB("hadoop");
            DBCollection collection = database.getCollection("calls");
            BasicDBObject document = new BasicDBObject();
            document.put("country", key.toString());
            document.put("ncalls", result.toString());
            collection.insert(document);

            // database.getCollectionNames().forEach(System.out::println);

        }catch (Exception e){
            System.out.println("goterr"+e);
        }


        context.write(key, result);

    }
}