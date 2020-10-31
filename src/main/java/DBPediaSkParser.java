import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBPediaSkParser extends Configured implements Tool {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private static Logger logger = LoggerFactory.getLogger(TokenizerMapper.class);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");
            Pattern idPattern = Pattern.compile("\"([0-9]+)\"\\^\\^");

            while(tokenizer.hasMoreTokens())
            {
                String page = tokenizer.nextToken();
                logger.info("Page name token: " + page);

                if (!tokenizer.hasMoreTokens()) return;
                String t = tokenizer.nextToken(); // Skip the second token
                logger.info("Second token: " + t);

                if (!tokenizer.hasMoreTokens()) return;
                String idString = tokenizer.nextToken(); // The third token is id
                logger.info("Third token: " + idString);

                Matcher matcher = idPattern.matcher(idString);
                if(matcher.find())
                {
                    idString = matcher.group(1);
                    logger.info("Matched id: " + idString);
                }

                int id = Integer.parseInt(idString);

                word.set(page);
                context.write(word, new IntWritable(id));

                if(!tokenizer.nextToken().equals("."))
                    logger.warn("There was not a dot in the end!");
            }
        }
    }

        public static class IdReducer extends Reducer<Text,IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

        private static Logger logger = LoggerFactory.getLogger(IdReducer.class);
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Iterator<IntWritable> itr = values.iterator();
            if (!itr.hasNext())
            {
                logger.warn("Empty reducer!");
                return;
            }

            logger.info("Reducing: " + key);
            result = itr.next();

            AvroKey<CharSequence> avroKey = new AvroKey<CharSequence>(key.toString());
            AvroValue<Integer> avroValue = new AvroValue<Integer>(result.get());
            context.write(avroKey, avroValue);

            if(itr.hasNext())
                logger.warn("There are several values for the key " + key);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MapReduceColorCount <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "DbPedia parser");
        job.setJarByClass(DBPediaSkParser.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(IdReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DBPediaSkParser(), args);
        System.exit(res);
    }
}