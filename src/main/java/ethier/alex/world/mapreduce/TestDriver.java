/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.addon.FilterListBuilder;
import ethier.alex.world.addon.PartitionBuilder;
import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.core.data.Partition;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**

 @author alex
 */
public class TestDriver {

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

            //This is testing the complement done by creating an arbitrary set of allowed combinations
            //Then creating filters out of them, applying and getting the new set of combinations (which is the complement)
            //Do the process again to get the complement of the complement and we should get back to what we started.

            //Note the complement is a great way to determine the efficiency of the algorithm.


            int ones = 9;
            int worldLength = 18;
//            int ones = 2;
//            int worldLength = 4;

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
            
        String outputPath = "/world/completed/";
        WorldRunner worldRunner = new WorldRunner(rootPartition, "/world", outputPath);
        
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5000000");
        conf.set(WorldRunner.RUN_INTITIAL_PARTITIONS_KEY, "" + 10000);
        int ret = ToolRunner.run(conf, worldRunner, args);//args must be passed in from shell.
        System.exit(ret);
    }
}
