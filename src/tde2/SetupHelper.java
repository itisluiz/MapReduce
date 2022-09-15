package tde2;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class SetupHelper
{
    public static boolean setupIO(Job job, String[] args)
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

    public static <C extends Mapper, K, V> void setupMapper(Job job, Class<C> mapper, Class<K> keyType, Class<V> valueType)
    {
        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(keyType);
        job.setMapOutputValueClass(valueType);
    }

    public static <C extends Reducer, K, V> void setupReducer(Job job, Class<C> reducer, Class<K> keyType, Class<V> valueType)
    {
        job.setReducerClass(reducer);
        job.setOutputKeyClass(keyType);
        job.setOutputValueClass(valueType);
    }

}
