/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import ethier.alex.world.mapreduce.data.ElementListWritable;
import ethier.alex.world.addon.CollectionByteSerializer;
import ethier.alex.world.mapreduce.memory.TaskMemoryManager;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class ResultReducer extends Reducer<Text, ElementListWritable, Text, Text>  {
    
    private static Logger logger = Logger.getLogger(ResultReducer.class);
    
    private TaskMemoryManager memoryManager;
    
        @Override
        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
            
            memoryManager = new TaskMemoryManager(context);
            logger.info("Reducer setup finished.");
        }

        @Override
        protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
            
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

            String serializedResults = CollectionByteSerializer.toString(byteCollection);
            memoryManager.setString(ResultExportRunner.RESULT_NAMED_OUTPUT, serializedResults);          
        }
}
