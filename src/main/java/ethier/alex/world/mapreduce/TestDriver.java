/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import com.google.common.base.Stopwatch;
import ethier.alex.world.addon.CollectionByteSerializer;
import ethier.alex.world.addon.FilterListBuilder;
import ethier.alex.world.addon.PartitionBuilder;
import ethier.alex.world.core.data.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
//import org.junit.Assert;

/**

 @author alex
 */
public class TestDriver {
    
    private static Logger logger = Logger.getLogger(TestDriver.class);

    public static void main(String[] args) throws Exception {
        TestDriver testDriver = new TestDriver();
        testDriver.drive(args);
    }

    public TestDriver() {
    }
    
    public void drive(String[] args) throws Exception {
        
        this.runComplementTest(args);
        
//        int[] radices = new int[4];
//        radices[0] = 3;
//        radices[1] = 2;
//        radices[2] = 3;
//        radices[3] = 2;
//        
//        FilterList filter1 = FilterListBuilder.newInstance()
//                .setOrdinals(new int[] {0, -1, -1, -1})
//                .getFilterList();
//        
//        FilterList filter2 = FilterListBuilder.newInstance()
//                .setOrdinals(new int[] {-1, -1, 1, -1})
//                .getFilterList();
//        
//        FilterList filter3 = FilterListBuilder.newInstance()
//                .setOrdinals(new int[] {-1, -1, 0, -1})
//                .getFilterList();
//        
//        FilterList filter4 = FilterListBuilder.newInstance()
//                .setOrdinals(new int[] {1, 1, 2, 0})
//                .getFilterList();
//        
//        Partition rootPartition = PartitionBuilder
//                .newInstance()
//                .setBlankWorld()
//                .setRadices(radices)
//                .addFilter(filter1)
//                .addFilter(filter2)
//                .addFilter(filter3)
//                .addFilter(filter4)
//                .getPartition();
//        
////        Properties props = new Properties();
////        props.setProperty(WorldRunner.WORK_DIRECTORY_KEY, "/world");
//        
//        WorldRunner worldRunner = new WorldRunner(rootPartition, "/world");
//        
//        Configuration conf = new Configuration();
//        int ret = ToolRunner.run(conf, worldRunner, args);
//        System.exit(ret);
    }
    
    public void runComplementTest(String[] args) throws Exception {
            System.out.println("");
            System.out.println("");
            System.out.println("********************************************");
            System.out.println("********      Complement Test      *********");
            System.out.println("********************************************");
            System.out.println("");
            System.out.println("");
            
            Stopwatch stopWatch =  new Stopwatch();
            stopWatch.start();

            //This is testing the complement done by creating an arbitrary set of allowed combinations
            //Then creating filters out of them, applying and getting the new set of combinations (which is the complement)
            //Do the process again to get the complement of the complement and we should get back to what we started.

            //Note the complement is a great way to determine the efficiency of the algorithm.


            int ones = 10;
            int worldLength = 20;
//            int ones = 5;
//            int worldLength = 10;

            Collection<FilterList> filters = new ArrayList<FilterList>();
            int[] radices = new int[worldLength];

            String breakStr = "";
            for (int i = 0; i < worldLength; i++) {
                    breakStr += '1';
                    radices[i] = 2;
            }

            String filterStr = "";
            int count = 0;
            while (!filterStr.equals(breakStr)) {

                filterStr = "" + Integer.toBinaryString(count);
                filterStr = StringUtils.leftPad(filterStr, worldLength, '0');
                int combOnes = StringUtils.countMatches(filterStr, "1");
                if (combOnes == ones) {
                    FilterList filter = FilterListBuilder.newInstance()
                            .setQuick(filterStr)
                            .getFilterList();

//                    logger.debug("Adding filter: " + filter);
                    filters.add(filter);
                }


                count++;
            }
            
            Partition rootPartition = PartitionBuilder.newInstance()
                    .setBlankWorld()
                    .setRadices(radices)
                    .addFilters(filters)
                    .getPartition();
            
        String completedPartitionsOutput = "/world/completed/";
        WorldRunner worldRunner = new WorldRunner(rootPartition, "/world", completedPartitionsOutput);
        
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5000000");
        conf.set(WorldRunner.RUN_INTITIAL_PARTITIONS_KEY, "" + 10000);
        int ret = 0;
        ret = ToolRunner.run(conf, worldRunner, args);//args must be passed in from shell.
        
        if(ret != 0) {
            throw new RuntimeException("Tool Runner failed.");
        }
        
        String serializedResultPath = "/world/results";
        logger.info("Running result export tool runner.");
        ResultExportRunner resultExportRunner = new ResultExportRunner(new Path(completedPartitionsOutput), serializedResultPath, "/world/defaultOutput");
        ret = ToolRunner.run(conf, resultExportRunner, args);
        
        if(ret != 0) {
            throw new RuntimeException("Tool Runner failed.");
        }
        
//        logger.info("Output result should be at: " + serializedResultPath);
        
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream inputStream = fileSystem.open(new Path(serializedResultPath));
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer, "UTF-8");
        String raw = writer.toString();
        Collection<byte[]> bytes = CollectionByteSerializer.toBytes(raw);
        logger.info("Collection size: " + bytes.size());
        Collection<ElementList> resultElements = new ArrayList<ElementList>();
        for(byte[] byteArray : bytes) {
            ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
            DataInput dataInput = new DataInputStream(bais);
            ElementList elementList = new ElementList(dataInput);
//            logger.info("Retreived element list: " + elementList.toString());
            resultElements.add(elementList);
        }

        // Now convert elements into complements.
        logger.info("Processing complements!");
        Collection<FilterList> complementFilters = generateComplementFilters(resultElements);

        Partition complementPartition = PartitionBuilder.newInstance()
                .setRadices(radices)
                .setBlankWorld()
                .addFilters(complementFilters)
                .getPartition();
        
        String complementPartitionsOutput = "/world/complementCompleted/";
        WorldRunner complementWorldRunner = new WorldRunner(complementPartition, "/world", complementPartitionsOutput);
        
//        Configuration complementConf = new Configuration();
//        conf.set("mapred.max.split.size", "5000000");
//        conf.set(WorldRunner.RUN_INTITIAL_PARTITIONS_KEY, "" + 10000);
        

        ret = ToolRunner.run(conf, complementWorldRunner, args);//args must be passed in from shell.
        if(ret != 0) {
            throw new RuntimeException("Tool Runner failed.");
        }
        
        String complementSerializedResultPath = "/world/complementResults";
        ResultExportRunner complementResultExportRunner = new ResultExportRunner(new Path(complementPartitionsOutput), complementSerializedResultPath, "/world/defaultOutput");
        ret = ToolRunner.run(conf, complementResultExportRunner, args);
        
        if(ret != 0) {
            throw new RuntimeException("Tool Runner failed.");
        }
        
//        logger.info("Output result should be at: " + serializedResultPath);
        
        inputStream = fileSystem.open(new Path(complementSerializedResultPath));
        writer = new StringWriter();
        IOUtils.copy(inputStream, writer, "UTF-8");
        raw = writer.toString();
        bytes = CollectionByteSerializer.toBytes(raw);
        logger.info("Collection size: " + bytes.size());
        Collection<ElementList> complementResultElements = new ArrayList<ElementList>();
        for(byte[] byteArray : bytes) {
            ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
            DataInput dataInput = new DataInputStream(bais);
            ElementList elementList = new ElementList(dataInput);
//            logger.info("Retreived element list: " + elementList.toString());
            complementResultElements.add(elementList);
        }
        
        Set<String> outputSet = new HashSet<String>();
        for(ElementList origList : complementResultElements) {
//                logger.debug(origList);

            String enumStr = origList.toString();
            int numOnes = StringUtils.countMatches(enumStr, "1");
            assertTrue(numOnes == ones);

            assertTrue(!outputSet.contains(enumStr));
            outputSet.add(enumStr);
        }

        int expectedCombinations = (int) MathUtils.binomialCoefficient(worldLength, ones);  
        assertTrue(expectedCombinations == outputSet.size());
     
        stopWatch.stop();
        int secondsRun = (int) stopWatch.elapsedTime(TimeUnit.SECONDS);
        
        logger.info(outputSet.size() + " complements found and reverted in " + secondsRun + " seconds");
    }
    
        public Collection<FilterList> generateComplementFilters(Collection<ElementList> elements) {
        
        Collection<FilterList> complementFilters = new ArrayList<FilterList>();
        for(ElementList resultCombination : elements) {
            logger.debug(resultCombination);
            int[] resultOrdinals = resultCombination.getOrdinals();

            Enum[] resultElementStates = resultCombination.getElementStates();
            FilterState[] complementFilterStates = new FilterState[resultElementStates.length];
            for(int i=0; i < resultElementStates.length; i++) {
                if(resultElementStates[i] == ElementState.ALL) {
                    complementFilterStates[i] = FilterState.ALL;
                } else if(resultElementStates[i] == ElementState.SET){
                    complementFilterStates[i] = FilterState.ONE;
                } else {
                    throw new RuntimeException("Invalid state reached.");
                }                    
            }

            FilterList complementFilter = FilterListBuilder.newInstance()
                    .setOrdinals(resultOrdinals)
                    .setFilterStates(complementFilterStates)
                    .getFilterList();

            complementFilters.add(complementFilter);                            
        }
        
        return complementFilters;
    }
        
    public void assertTrue(boolean bool) {
        if(bool == true) {
            return;
        } else {
            throw new RuntimeException("Assertion Failed.");
        }
    }
}
