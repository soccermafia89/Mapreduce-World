/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.addon;

import com.google.common.base.Stopwatch;
import ethier.alex.world.addon.FilterListBuilder;
import ethier.alex.world.addon.PartitionBuilder;
import ethier.alex.world.core.data.*;
import ethier.alex.world.mapreduce.core.WorldRunner;
import ethier.alex.world.mapreduce.processor.DistributedProcessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class TestProcessor {
    
    private static Logger logger = Logger.getLogger(TestProcessor.class);

    public static void main(String[] args) throws Exception {
        TestProcessor testDriver = new TestProcessor();
        testDriver.drive(args);
    }

    public TestProcessor() {
    }

    public void drive(String[] args) throws Exception {
        this.runComplementTest(args);
    }

    public void runComplementTest(String[] args) throws Exception {
        System.out.println("");
        System.out.println("");
        System.out.println("********************************************");
        System.out.println("********      Complement Test      *********");
        System.out.println("********************************************");
        System.out.println("");
        System.out.println("");

        Stopwatch stopWatch = new Stopwatch();
        stopWatch.start();

        //This is testing the complement done by creating an arbitrary set of allowed combinations
        //Then creating filters out of them, applying and getting the new set of combinations (which is the complement)
        //Do the process again to get the complement of the complement and we should get back to what we started.

        //Note the complement is a great way to determine the efficiency of the algorithm.


//        int ones = 10;
//        int worldLength = 20;
        int ones = 8;
        int worldLength = 16;

        int[] radices = new int[worldLength];
        for (int i = 0; i < worldLength; i++) {
            radices[i] = 2;
        }

        Partition rootPartition = generateBinomialPartition(ones, worldLength);
        
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5000000");
        conf.set(WorldRunner.RUN_INTITIAL_PARTITIONS_KEY, "" + 10000);
        
        DistributedProcessor distributedProcessor = new DistributedProcessor(rootPartition, "/world", conf, args);
        distributedProcessor.runAll();
        Collection<ElementList> resultElements = distributedProcessor.getCompletedPartitions();

        // Now convert elements into complements.
        logger.info("Processing complements!");
        Collection<FilterList> complementFilters = generateComplementFilters(resultElements);

        Partition complementPartition = PartitionBuilder.newInstance()
                .setRadices(radices)
                .setBlankWorld()
                .addFilters(complementFilters)
                .getPartition();
        
        DistributedProcessor complementDistributedProcessor = new DistributedProcessor(complementPartition, "/world", conf, args);
        complementDistributedProcessor.runAll();
        Collection<ElementList> complementResultElements = complementDistributedProcessor.getCompletedPartitions();
//        
//        logger.info("Found: " + complementResultElements.size() + " complement results");
//        for(ElementList complementResult: complementResultElements) {
//            logger.info("Found complement result: " + complementResult);
//        }

        Set<String> outputSet = new HashSet<String>();
        for (ElementList origList : complementResultElements) {
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
    
    public static Partition generateBinomialPartition(int ones, int worldLength) {
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
                FilterList filter = FilterListBuilder.newInstance().setQuick(filterStr).getFilterList();

//                    logger.debug("Adding filter: " + filter);
                filters.add(filter);
            }


            count++;
        }

        return PartitionBuilder.newInstance()
                .setBlankWorld()
                .setRadices(radices)
                .addFilters(filters)
                .getPartition();
    }

    public static Collection<FilterList> generateComplementFilters(Collection<ElementList> elements) {

        Collection<FilterList> complementFilters = new ArrayList<FilterList>();
        for (ElementList resultCombination : elements) {
            logger.debug(resultCombination);
            int[] resultOrdinals = resultCombination.getOrdinals();

            Enum[] resultElementStates = resultCombination.getElementStates();
            FilterState[] complementFilterStates = new FilterState[resultElementStates.length];
            for (int i = 0; i < resultElementStates.length; i++) {
                if (resultElementStates[i] == ElementState.ALL) {
                    complementFilterStates[i] = FilterState.ALL;
                } else if (resultElementStates[i] == ElementState.SET) {
                    complementFilterStates[i] = FilterState.ONE;
                } else {
                    throw new RuntimeException("Invalid state reached.");
                }
            }

            FilterList complementFilter = FilterListBuilder.newInstance().setOrdinals(resultOrdinals).setFilterStates(complementFilterStates).getFilterList();

            complementFilters.add(complementFilter);
        }

        return complementFilters;
    }

    public void assertTrue(boolean bool) {
        if (bool == true) {
            return;
        } else {
            throw new RuntimeException("Assertion Failed.");
        }
    }
}
