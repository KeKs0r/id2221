package sics;

import java.io.IOException;
import java.util.*;

import mrdp.utils.MRDPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopTen {

    /**
     * taken from : https://blog.pivotal.io/pivotal/products/how-hadoop-mapreduce-can-transform-how-you-build-top-ten-lists
     * @param <Text>
     */
    static class Item<Text> implements Comparable<Item<Text>> {
        int score;
        Text c;

        public Item(int score, Text c) {
            this.score = score;
            this.c = c;
        }

        @Override
        public int compareTo(Item<Text> i) {
            int x = -Double.compare(score, i.score);
            return x;
        }
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        // Treeset alone overwrites records with the same reputition
        private SortedSet<Item<Text>> repToRecordMap = new TreeSet<Item<Text>>();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
             * The xml transform is taken from the book
             * MapReduce Design Patterns: Building Effective Algorithms and Analytics for Hadoop and Other Systems
             */
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            if (parsed.containsKey("Reputation") && parsed.containsKey("AccountId")) {
                String userId = parsed.get("AccountId");
                String reputation = parsed.get("Reputation");


                // Add this record to our map with the reputation as the key
                Integer weight = Integer.parseInt(reputation);
                Text userIdText = new Text(reputation + ";" +userId);
                repToRecordMap.add(new Item<Text>(weight, userIdText));

                // If we have more than ten records, remove the one with the lowest reputation.
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.last());
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for (Item item : repToRecordMap) {
                context.write(NullWritable.get(), (Text) item.c);
            }
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        // Overloads the comparator to order the reputations in descending order
        private SortedSet<Item<Text>> repToRecordMap = new TreeSet<Item<Text>>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] input = value.toString().split(";");

                String reputation = input[0];
                String userId = input[1];
                // Add this record to our map with the reputation as the key
                Integer weight = Integer.parseInt(reputation);
                Text userIdText = new Text("Rep:"+reputation + "; User:" +userId);
                repToRecordMap.add(new Item<Text>(weight, userIdText));

                // If we have more than ten records, remove the one with the lowest reputation.
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.last());
                }
            }

            for (Item item : repToRecordMap) {
                context.write(NullWritable.get(), (Text) item.c);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top ten");
        job.setJarByClass(TopTen.class);

        job.setMapperClass(TopTenMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TopTenReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
