import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import avro.DbPage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBPediaSkParser {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private static Logger logger = LoggerFactory.getLogger(TokenizerMapper.class);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");

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

                idString = idString.substring(1, 4); // TODO id parsing
                int id = Integer.parseInt(idString);

                word.set(page);
                context.write(word, new IntWritable(id));

                if(!tokenizer.nextToken().equals("."))
                    logger.warn("There was not a dot in the end!");
            }
        }
    }

    public static class IdReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private static Logger logger = LoggerFactory.getLogger(IdReducer.class);
        private IntWritable result = new IntWritable();

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

            key = new Text(key.toString().trim());
            context.write(key, result);

            if(itr.hasNext())
                logger.warn("There are several values for the key " + key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "DBPedia SK parsing");
        job.setJarByClass(DBPediaSkParser.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IdReducer.class);
        job.setReducerClass(IdReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputFile = new Path(args[0]);
        Path outputFile = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        int result = job.waitForCompletion(true) ? 0 : 1;

        parseOutputAndStoreAvro(outputFile);

        System.exit(result);
    }

    private static void parseOutputAndStoreAvro(Path outputFile){
        DbPage dbPage = new DbPage();
    }
}
