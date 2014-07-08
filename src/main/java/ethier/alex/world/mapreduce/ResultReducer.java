/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.addon.CollectionByteSerializer;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class ResultReducer extends Reducer<Text, ElementListWritable, Text, Text>  {
    
    private static Logger logger = Logger.getLogger(ResultReducer.class);
    Path outputPath;
    
//    private HdfsOutput outputWriter;
    
            @Override
        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
//            outputWriter = new HdfsOutput(context);
            outputPath = new Path(context.getConfiguration().get(ResultExportRunner.RESULT_OUTPUT_KEY));
            logger.info("Reducer setup finished.");
        }

        @Override
        protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
            
//            outputWriter.close();
            super.cleanup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<ElementListWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("Reducing Key: " + key.toString());

            Iterator<ElementListWritable> it = values.iterator();
            Collection<byte[]> byteCollection = new ArrayList<byte[]>();
            while(it.hasNext()) {
                ElementListWritable elementListWritable = it.next();
                logger.info("Writing elements: " + elementListWritable.getElementList().toString());
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutput dataOutput = new DataOutputStream(baos);
                elementListWritable.write(dataOutput);
                byte[] serializedElementList = baos.toByteArray();
                byteCollection.add(serializedElementList);
            }
            
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            FSDataOutputStream outStream = fileSystem.create(outputPath);
            String serializedResults = CollectionByteSerializer.toString(byteCollection);
            outStream.write(serializedResults.getBytes());         
            outStream.close();            
        }
}
