import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import tde2.Transaction;

import java.io.File;
import java.io.IOException;

public class Main
{
    private static boolean setupIO(String[] args, Job job)
    {
        if (args.length < 2)
        {
            System.out.println("Missing input and/or output parameters");
            return false;
        }

        File in = new File(args[0]);
        if (!in.exists())
        {
            System.out.println("Input file '" + args[0] + "' doesn't exist");
            return false;
        }

        File out = new File(args[1]);
        if (out.exists())
        {
            try
            {
                FileUtils.forceDelete(out);
            }
            catch (IOException e)
            {
                System.out.println("Failed to delete current output files at '" + args[1] + "'");
                return false;
            }
        }

        try
        {
            FileInputFormat.addInputPath(job, new Path(in.toString()));
            FileOutputFormat.setOutputPath(job, new Path(out.toString()));
        }
        catch (IOException e)
        {
            System.out.println("Failed to setup job for given input and output");
            return false;
        }

        return true;
    }

    private static <C extends Mapper, K, V> void setupMapper(Job job, Class<C> mapper, Class<K> keyType, Class<V> valueType)
    {
        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(keyType);
        job.setMapOutputValueClass(valueType);
    }

    private static <C extends Reducer, K, V> void setupReducer(Job job, Class<C> reducer, Class<K> keyType, Class<V> valueType)
    {
        job.setReducerClass(reducer);
        job.setOutputKeyClass(keyType);
        job.setOutputValueClass(valueType);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration(), "tde2");

        if (!setupIO(args, job))
            return;

        job.setJarByClass(Main.class);

        setupMapper(job, Map.class, Text.class, IntWritable.class);
        setupReducer(job, Reduce.class, Text.class, IntWritable.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            // Wrapper da linha CSV, cada campo pode ser obtido através de um getter
            Transaction t = new Transaction(value.toString());

            // Mapear todas as entradas para (País - 1 ocorrência)
            con.write(new Text(t.getCountry()), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            // Somar e reduzir as entradas para obter total de transações envolvedo país
            int total = 0;

            for (IntWritable value : values)
                total += value.get();

            con.write(key, new IntWritable(total));
        }
    }
}
