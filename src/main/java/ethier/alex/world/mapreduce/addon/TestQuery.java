/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.addon;

import ethier.alex.world.addon.FilterListBuilder;
import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.core.data.Partition;
import ethier.alex.world.core.processor.Processor;
import ethier.alex.world.mapreduce.core.WorldRunner;
import ethier.alex.world.mapreduce.processor.DistributedProcessor;
import ethier.alex.world.mapreduce.query.QueryRunner;
import ethier.alex.world.mapreduce.query.WorldSizeRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class TestQuery {
    
    private static Logger logger = Logger.getLogger(TestQuery.class);
    
    public static void main(String[] args) throws Exception {
        TestQuery testQuery = new TestQuery();
        testQuery.drive(args);
    }
    
    public TestQuery() {
        
    }
    
    public void drive(String[] args) throws Exception {
        
        int ones = 4;
        int worldLength = 8;
        
        Partition rootPartition = TestProcessor.generateBinomialPartition(ones, worldLength);
        
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5000000");
        conf.set(WorldRunner.RUN_INTITIAL_PARTITIONS_KEY, "" + 10000);
        
        logger.info("TMP DISABLED DISTRIBUTED PROCESSOR.");
//        logger.info("Running distributed processor.");
//        Processor distributedProcessor = new DistributedProcessor(rootPartition, "/world", conf, args);
//        distributedProcessor.runAll();
        
        logger.info("Running World Size Runner.");
        WorldSizeRunner worldSizeRunner = new WorldSizeRunner("/world/completed/", "/world/default", rootPartition.getRadices());
        int ret = ToolRunner.run(conf, worldSizeRunner, args);

        if(ret != 0) {
            throw new RuntimeException("Tool Runner failed.");
        }
        
//        logger.info("TMP EXIT SYSTEM!");
//        System.exit(0);
        
        String worldSizeString = worldSizeRunner.getWorldSize();
        logger.info("Exported world size: " + worldSizeString);
        
        String queryFilter = "1";
        for(int i=0; i < worldLength-1; i++) {
            queryFilter += "*";
        }
        
        FilterList filterList = FilterListBuilder.newInstance()
                .setQuick(queryFilter)
                .getFilterList();
        
        logger.info("Running query runner.");
        QueryRunner queryRunner = new QueryRunner(worldSizeString, filterList, "/world/completed/", rootPartition.getRadices(), "/world");
        ret = ToolRunner.run(conf, queryRunner, args);

        if(ret != 0) {
            throw new RuntimeException("Tool Runner failed.");
        }
    }
    
//    public String worldSizeExport(String worldSizePath, Configuration conf) throws IOException {
//        
//        FileSystem fileSystem = FileSystem.get(conf);
//        FSDataInputStream inputStream = fileSystem.open(new Path(worldSizePath));
//        StringWriter writer = new StringWriter();
//        IOUtils.copy(inputStream, writer, "UTF-8");
//        String worldSizeString = writer.toString();
//        
//        return worldSizeString;
//    }
}
