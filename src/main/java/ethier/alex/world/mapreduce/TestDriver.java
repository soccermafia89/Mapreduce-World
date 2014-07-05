/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.addon.FilterListBuilder;
import ethier.alex.world.addon.PartitionBuilder;
import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.core.data.Partition;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
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
        
        int[] radices = new int[4];
        radices[0] = 3;
        radices[1] = 2;
        radices[2] = 3;
        radices[3] = 2;
        
        FilterList filter1 = FilterListBuilder.newInstance()
                .setOrdinals(new int[] {0, -1, -1, -1})
                .getFilterList();
        
        FilterList filter2 = FilterListBuilder.newInstance()
                .setOrdinals(new int[] {-1, -1, 1, -1})
                .getFilterList();
        
        FilterList filter3 = FilterListBuilder.newInstance()
                .setOrdinals(new int[] {-1, -1, 0, -1})
                .getFilterList();
        
        FilterList filter4 = FilterListBuilder.newInstance()
                .setOrdinals(new int[] {1, 1, 2, 0})
                .getFilterList();
        
        Partition rootPartition = PartitionBuilder
                .newInstance()
                .setBlankWorld()
                .setRadices(radices)
                .addFilter(filter1)
                .addFilter(filter2)
                .addFilter(filter3)
                .addFilter(filter4)
                .getPartition();
        
        Properties props = new Properties();
        props.setProperty(WorldRunner.WORK_DIRECTORY_KEY, "/world");
        
        WorldRunner worldRunner = new WorldRunner(rootPartition, props);
        
        Configuration conf = new Configuration();
        int ret = ToolRunner.run(conf, worldRunner, args);
        System.exit(ret);
    }
}
