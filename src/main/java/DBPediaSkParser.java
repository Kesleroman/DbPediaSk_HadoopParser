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

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

        private static Logger logger = LoggerFactory.getLogger(TokenizerMapper.class);
        private static Pattern idPattern = Pattern.compile("\"([0-9]+)\"\\^\\^");
        private static Pattern labelPattern = Pattern.compile("\"(.+)\"@sk");
        private Text returnKey = new Text();
        private Text returnValue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueString = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(valueString, " ");


            String page = tokenizer.nextToken();
            logger.info("Page name token: " + page);

            if (!tokenizer.hasMoreTokens()) return;
            String dataType = tokenizer.nextToken();

            Matcher matcher = getAppropriateMatcher(dataType, value.toString());
            if (matcher == null) {
                logger.error("Unknown data type: " + dataType);
                return;
            }

            String matchedData;
            if(matcher.find()) {
                matchedData = matcher.group(1);
                logger.info("Matched data: " + matchedData);
            }
            else {
                logger.warn("Nothing were matched.");
                return;
            }

            returnKey.set(page);
            returnValue.set(matchedData);
            context.write(returnKey, returnValue);

            if(!valueString.endsWith("."))
                logger.warn("There was not a dot in the end!");
        }

        private Matcher getAppropriateMatcher(String dataType, String data){
            if (dataType.equals("<http://www.w3.org/2000/01/rdf-schema#label>"))
                return labelPattern.matcher(data);
            else if (dataType.equals("<http://dbpedia.org/ontology/wikiPageID>"))
                return idPattern.matcher(data);

            return null;
        }
    }

        public static class IdReducer extends Reducer<Text,Text, AvroKey<CharSequence>, AvroValue<CharSequence>> {

        private static Logger logger = LoggerFactory.getLogger(IdReducer.class);

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Iterator<Text> itr = values.iterator();
            if (!itr.hasNext())
            {
                logger.warn("Empty reducer!");
                return;
            }

//            logger.info("Reducing: " + key);
            StringBuilder resultBuilder = new StringBuilder();
            while (itr.hasNext()){
                Text text = itr.next();
                resultBuilder.append(text.toString());
            }

            AvroKey<CharSequence> avroKey = new AvroKey<CharSequence>(key.toString());
            AvroValue<CharSequence> avroValue = new AvroValue<CharSequence>(resultBuilder.toString());
            context.write(avroKey, avroValue);

            if(itr.hasNext())
                logger.warn("There are several values for the key " + key);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Parameters: <input path> <output path>");
            return 1;
        }

        Job job = Job.getInstance(getConf(), "DbPedia parser");
        job.setJarByClass(DBPediaSkParser.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(IdReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.STRING));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DBPediaSkParser(), args);
        System.exit(res);
    }
}