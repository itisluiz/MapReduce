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
    /*
        Permite um número arbitrário de jobs para serem executados em sequência.

        Se 'fresh', então configura o IO para os inputs e outputs passados em args,
        deletando quaisquer outputs anteriores.

        Se não-'fresh', usa como input o output passado em args, movendo o output
        anterior de '/part-r-00000' para '/../job-intermediary' contendo este output,
        livrando o diretório de output para poder ser usado como output novamente.
     */

    public static boolean setupIO(Job job, String[] args, boolean fresh)
    {
        if (args.length < 2)
        {
            System.out.println("Missing input and/or output parameters");
            return false;
        }

        File in = new File(fresh ? args[0] : args[1].concat("/../job-intermediary"));
        File out = new File(args[1]);

        if (!fresh)
        {
            File intermediary = new File(args[1].concat("/part-r-00000"));

            if (in.exists() && !in.delete())
            {
                System.out.println("Failed to delete '" + in.getAbsolutePath() + "'");
                return false;
            }

            if (!intermediary.exists() || !intermediary.renameTo(in))
            {
                System.out.println("Failed to move '" + intermediary.getAbsolutePath() + "' to '" + in.getAbsolutePath() + "'");
                return false;
            }
        }

        if (!in.exists())
        {
            System.out.println("Input file '" + in.getAbsolutePath() + "' doesn't exist");
            return false;
        }

        if (out.exists())
        {
            try
            {
                FileUtils.forceDelete(out);
            }
            catch (IOException e)
            {
                System.out.println("Failed to delete '" + out.getAbsolutePath() + "'");
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

    public static boolean setupIO(Job job, String[] args)
    {
        return setupIO(job, args, true);
    }

    public static <C extends Mapper, K, V> void setupMapper(Job job, Class<C> mapper, Class<K> keyType, Class<V> valueType)
    {
        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(keyType);
        job.setMapOutputValueClass(valueType);
    }

    public static <C extends Reducer> void setupCombiner(Job job, Class<C> combiner)
    {
        job.setCombinerClass(combiner);
    }

    public static <C extends Reducer, K, V> void setupReducer(Job job, Class<C> reducer, Class<K> keyType, Class<V> valueType)
    {
        job.setReducerClass(reducer);
        job.setOutputKeyClass(keyType);
        job.setOutputValueClass(valueType);
    }

}
