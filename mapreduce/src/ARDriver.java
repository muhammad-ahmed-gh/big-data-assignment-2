import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ARDriver {
    public static class StatsWritable implements Writable {

        private int count;
        private double sum;
        private double min;
        private double max;
        public StatsWritable() {
            this.count = 0;
            this.sum = 0;
            this.min = Double.POSITIVE_INFINITY;
            this.max = Double.NEGATIVE_INFINITY;
        }
        public StatsWritable(int count, double sum, double min, double max) {
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }
        public int getCount() {
            return count;
        }
        public double getSum() {
            return sum;
        }
        public double getMin() {
            return min;
        }
        public double getMax() {
            return max;
        }
        public void set(int count, double sum, double min, double max) {
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(count);
            out.writeDouble(sum);
            out.writeDouble(min);
            out.writeDouble(max);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readInt();
            sum = in.readDouble();
            min = in.readDouble();
            max = in.readDouble();
        }
        @Override
        public String toString() {
            double average = sum / count;
            return count + "," +
                    String.format("%.2f", average) + "," +
                    String.format("%.1f", min) + "," +
                    String.format("%.1f", max);
        }
    }
    // MAPPER
    public static class MapperClass
            extends Mapper<LongWritable, Text, Text, StatsWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }
            String line = value.toString();
            StringBuilder cleanedLine = new StringBuilder();
            boolean insideQuotes = false;
            for (char c : line.toCharArray()) {

                if (c == '"') {
                    insideQuotes = !insideQuotes;
                }

                if (c == ',' && insideQuotes) {
                    cleanedLine.append('|');
                } else {
                    cleanedLine.append(c);
                }
            }
            String[] fields = cleanedLine.toString().split(",");
            if (fields.length < 8) {
                return;
            }
            try {
                String genreField =
                        fields[4].replace("\"", "").trim();

                String yearField =
                        fields[3].replace("\"", "").trim();

                String typeField =
                        fields[9].replace("\"", "").trim();

                String ratingField =
                        fields[11].replace("\"", "").trim();
                if (genreField.isEmpty() ||
                        yearField.isEmpty() ||
                        typeField.isEmpty() ||
                        ratingField.isEmpty()) {
                    return;
                }
                String yearOnly =
                        yearField.split("-")[0].trim();

                int year = Integer.parseInt(yearOnly);
                String decade =
                        (year / 10) * 10 + "s";
                double rating =
                        Double.parseDouble(
                                ratingField.replaceAll("[^0-9.]", "")
                        );
                if (rating < 0 || rating > 10) {
                    return;
                }
                String[] genres = genreField.split("\\|");
                for (String genre : genres) {
                    genre = genre.trim();
                    if (!genre.isEmpty()) {
                        String outputKey =
                                typeField + "," +
                                        genre + "," +
                                        decade;

                        StatsWritable stats =
                                new StatsWritable(
                                        1,
                                        rating,
                                        rating,
                                        rating
                                );
                        context.write(
                                new Text(outputKey),
                                stats
                        );
                    }
                    break;
                }

            } catch (Exception e) {

            }
        }
    }

    // COMBINER
    public static class CombinerClass
            extends Reducer<Text, StatsWritable, Text, StatsWritable> {

        @Override
        public void reduce(Text key,
                           Iterable<StatsWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int totalCount = 0;
            double totalSum = 0;
            double globalMin = Double.POSITIVE_INFINITY;
            double globalMax = Double.NEGATIVE_INFINITY;

            for (StatsWritable val : values) {

                totalCount += val.getCount();
                totalSum += val.getSum();
                globalMin =
                        Math.min(globalMin, val.getMin());
                globalMax =
                        Math.max(globalMax, val.getMax());
            }

            StatsWritable combinedStats =
                    new StatsWritable(
                            totalCount,
                            totalSum,
                            globalMin,
                            globalMax
                    );

            context.write(key, combinedStats);
        }
    }
    // REDUCER
    public static class ReducerClass
            extends Reducer<Text, StatsWritable, Text, Text> {

        @Override
        public void reduce(Text key,
                           Iterable<StatsWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            int totalCount = 0;
            double totalSum = 0;
            double globalMin = Double.POSITIVE_INFINITY;
            double globalMax = Double.NEGATIVE_INFINITY;
            for (StatsWritable val : values) {

                totalCount += val.getCount();

                totalSum += val.getSum();

                globalMin =
                        Math.min(globalMin, val.getMin());

                globalMax =
                        Math.max(globalMax, val.getMax());
            }
            if (totalCount == 0) {
                return;
            }
            double average = totalSum / totalCount;
            String result =
                    "," +
                            totalCount + "," +
                            String.format("%.2f", average) + "," +
                            String.format("%.1f", globalMin) + "," +
                            String.format("%.1f", globalMax);

            context.write(key, new Text(result));
        }
    }
    // DRIVER
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                    "Usage: ARDriver <input path> <output path>"
            );
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job =
                Job.getInstance(conf, "Movie Statistics");
        job.setJarByClass(ARDriver.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(
                StatsWritable.class
        );
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );
        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}