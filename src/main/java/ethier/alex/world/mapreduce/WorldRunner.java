/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.core.data.Partition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**

 @author alex
 */
// Should read in properties 
public class WorldRunner extends Configured implements Tool {

    Partition rootPartition;

    public WorldRunner(Partition myPartition) {
        rootPartition = myPartition;
    }

    @Override
    public int run(String[] strings) throws Exception {

        Path rootPath = new Path("/world/root");
        Configuration fsConf = new Configuration();
//        fsConf.set("fs.default.name", uri);
        FileSystem fileSystem = FileSystem.get(fsConf);
        System.out.println("Clearing output file.");
        fileSystem.delete(rootPath, true);

        SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(rootPath);
        SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(PartitionWritable.class);
        SequenceFile.Writer writer = SequenceFile.createWriter(fsConf, optPath, optKey, optVal);
        writer.append(new Text(rootPartition.printElements()), new PartitionWritable(rootPartition));

        Configuration conf = getConf();

        Job job = new Job(conf);
//        job.setJarByClass(WorldRunner.class);
        job.setJobName(this.getClass().getName());
        job.setMapperClass(WorldMapper.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, rootPath);
        
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "seq1", SequenceFileOutputFormat.class, Text.class, PartitionWritable.class);
        MultipleOutputs.addNamedOutput(job, "seq2", SequenceFileOutputFormat.class, Text.class, ElementListWritable.class);
        Path outputPath = new Path("/world/output");
        fileSystem.delete(outputPath, true);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
