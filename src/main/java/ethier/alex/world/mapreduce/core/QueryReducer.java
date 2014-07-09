///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package ethier.alex.world.mapreduce.core;
//
//import ethier.alex.world.mapreduce.data.ElementListWritable;
//import java.io.IOException;
//import java.util.Iterator;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.log4j.Logger;
//
///**
//
// @author alex
// */
//public class QueryReducer extends Reducer<Text, ElementListWritable, Text, Text> {
//        
//    private static Logger logger = Logger.getLogger(QueryReducer.class);
//    
//    @Override
//    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) {
//
//        logger.info("Reducer setup finished.");
//    }
//
//    @Override
//    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
//
//        super.cleanup(context);
//    }
//
//    @Override
//    protected void reduce(Text key, Iterable<ElementListWritable> values, Context context) throws IOException, InterruptedException {
//        logger.info("Reducing Key: " + key.toString());
//
//        Iterator<ElementListWritable> it = values.iterator();
//        Collection<byte[]> byteCollection = new ArrayList<byte[]>();
//        while(it.hasNext()) {
//            ElementListWritable elementListWritable = it.next();
//            logger.info("Writing elements: " + elementListWritable.getElementList().toString());
//
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            DataOutput dataOutput = new DataOutputStream(baos);
//            elementListWritable.write(dataOutput);
//            byte[] serializedElementList = baos.toByteArray();
//            byteCollection.add(serializedElementList);
//        }
//
//        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
//        FSDataOutputStream outStream = fileSystem.create(outputPath);
//        String serializedResults = CollectionByteSerializer.toString(byteCollection);
//        outStream.write(serializedResults.getBytes());         
//        outStream.close();            
//    }
//}
