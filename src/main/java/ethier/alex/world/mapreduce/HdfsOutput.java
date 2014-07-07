/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

/**

 @author alex
 */
// This class binds the named output to an output path.
// The implementation of the MultipleOutputs class is complete garbage.  Not only does it mix static and non-static methods in a confusing way,
// It also allows the specification of the output path to occur within the mapper itself, not when the named output is defined.
// Job I/O should not be designated within mappers, they should be configured before mappers are run.
public class HdfsOutput {

    private static Logger logger = Logger.getLogger(WorldMapper.class);
    private static final String NAMED_OUTPUTS_KEY = "ethier.alex.world.mapreduce.hdfs.named.outputs";
    private static final String NAMED_OUTPUT_BASE_KEY = "ethier.alex.world.mapreduce.hdfs.named.output";

    private MultipleOutputs mos;
    private TaskInputOutputContext context;

    public HdfsOutput(TaskInputOutputContext myContext) {

        mos = new MultipleOutputs(myContext);
        context = myContext;

        logger.info("File Manager Setup Successfully!");
    }

    public static void setupDefaultOutput(Job job, Path defaultOutputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        fileSystem.delete(defaultOutputPath, true);
        SequenceFileOutputFormat.setOutputPath(job, defaultOutputPath);
    }

    public static void clearNamedOutputs(Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        String namedOutputs = conf.get(NAMED_OUTPUTS_KEY);
        String[] namedOutputsArray = StringUtils.split(namedOutputs, ",");
        for (String namedOutput : namedOutputsArray) {
            String namedOutputPath = conf.get(NAMED_OUTPUT_BASE_KEY + "." + namedOutput);
            fileSystem.delete(new Path(namedOutputPath), true);
        }
    }

    public static void addNamedOutput(Job job, String namedOutput, String outputPath, Class<? extends OutputFormat> outputFormatClass, Class<?> keyClass, Class<?> valueClass) {

        appendNamedOutputConfiguration(job, namedOutput);
        job.getConfiguration().set(NAMED_OUTPUT_BASE_KEY + "." + namedOutput, outputPath);

        MultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, keyClass, valueClass);
    }

    private static void appendNamedOutputConfiguration(Job job, String namedOutput) {

        Configuration conf = job.getConfiguration();

        String namedOutputs = conf.get(NAMED_OUTPUTS_KEY);
        if (namedOutputs == null || namedOutputs.isEmpty()) {
            namedOutputs = namedOutput;
        } else {
            namedOutputs = namedOutputs + "," + namedOutput;
        }

        conf.set(NAMED_OUTPUTS_KEY, namedOutputs);
    }

    public <K extends Object, V extends Object> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {

        String namedOutputPath = context.getConfiguration().get(NAMED_OUTPUT_BASE_KEY + "." + namedOutput);

//        logger.info("Writing to: " + namedOutputPath);
        mos.write(namedOutput, key, value, namedOutputPath + "/" + namedOutput);
    }

    public static String getNamedOutputPath(Job job, String namedOutput) {
        return job.getConfiguration().get(NAMED_OUTPUT_BASE_KEY + "." + namedOutput);
    }

    public void close() throws IOException, InterruptedException {
        mos.close();
    }
}
