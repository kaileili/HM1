import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Floatwritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class count {

    private static String Avg = "Avg";
    private static String SIZE = "SIZE";

    private static Map<String, Integer> getListAvgValue(Iterable<FloatWritable> LifeExpectancy) {
        Map<String, Integer> resultMap = new HashMap<>();
        int max = Integer.MIN_VALUE;
        int size = 0;
        for (FloatWritable  : LifeExpectancy {
            size++;
            int priceInt = price.get();
            if (max < ExpInt) {
                max = ExpInt;
            }
        }
        resultMap.put(Avgex, max);
        resultMap.put(SIZE, size);
        return resultMap;
    }

    public static class AvgExpReducer extends Reducer<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object offset, Text rows, Context context) throws IOException, InterruptedException {

            
            String[] cols = rows.toString().split(",");
            
            context.write(new Text(words[2]), new FloatWritable(Float.parseFloat(words[7])));
            }

        }

    }

    @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (FloatWritable value : values) {
                sum += value.get();
            }
            if (sum > 10000) {
                context.write(key, new LongWritable(sum));
            }
        }
    }

    public static class AvgExpReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text barName, FloatWritable<FloatWritable> prices, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> statInfo = getListAvgValue();
            if (statInfo.get(MAX_VALUE) <= 5) {
                context.write(barName, new IntWritable(statInfo.get(SIZE)));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        for (int i = 0; i < otherArgs.length; i++) {
            System.out.println(otherArgs[i]);
        }
        Job job = Job.getInstance(conf, "inf_hw5");
        job.setJarByClass(count.class);
        job.setMapperClass(AvgExpMapper.class);
        job.setReducerClass(AvgExpReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}