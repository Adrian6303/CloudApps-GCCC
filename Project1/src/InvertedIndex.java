import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.LongWritable;

public class InvertedIndex {


    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Set<String> stopWords = new HashSet<>();
        private Text wordKey  = new Text();
        private Text location = new Text();

        private String currentFile = "";
        private long lineNumber    = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load stopwords from a distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("stopwords.txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }

            // Get filename and reset line counter for this split
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            currentFile = fileSplit.getPath().getName();
            lineNumber  = 0;
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Increment line number for every line read
            lineNumber++;

            StringTokenizer tokenizer = new StringTokenizer(
                    value.toString(), " \t\r\n\f\"',.:;!?()[]{}#$*-_+/\\<>@%&=~`^|0123456789");

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toLowerCase().trim();

                if (token.isEmpty())           continue;
                if (token.length() < 2)        continue;
                if (stopWords.contains(token)) continue;

                wordKey.set(token);
                location.set(currentFile + ":" + lineNumber);
                context.write(wordKey, location);
            }
        }
    }


    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            LinkedHashSet<String> locations = new LinkedHashSet<>();
            for (Text val : values) {
                locations.add(val.toString());
            }

            StringBuilder sb = new StringBuilder();
            for (String loc : locations) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(loc);
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: InvertedIndex <input_dir> <output_dir> <stopwords_file>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[2] + "#stopwords.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}