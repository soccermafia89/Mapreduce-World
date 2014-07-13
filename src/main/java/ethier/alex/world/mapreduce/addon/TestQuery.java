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
import ethier.alex.world.mapreduce.query.DistributedQuery;
import ethier.alex.world.mapreduce.query.QueryRunner;
import ethier.alex.world.mapreduce.query.WorldSizeRunner;
import java.util.ArrayList;
import java.util.Collection;
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
        
//        logger.info("TMP DISABLED DISTRIBUTED PROCESSOR.");
        logger.info("Running distributed processor.");
        Processor distributedProcessor = new DistributedProcessor(rootPartition, "/world", conf, args);
        distributedProcessor.runAll();
        
        logger.info("Running Query.");
        DistributedQuery distributedQuery = new DistributedQuery("/world", rootPartition.getRadices(), conf, args);
        
        logger.info("World Size: " + distributedQuery.getWorldSize().toPlainString());
        
        String queryFilter = "1";
        for(int i=0; i < worldLength-1; i++) {
            queryFilter += "*";
        }
        
        FilterList filterList = FilterListBuilder.newInstance()
                .setQuick(queryFilter)
                .getFilterList();
        
        double queryValue = distributedQuery.query(filterList);
        
        logger.info("Query: " + filterList + " => " + queryValue);
        queryFilter = "0";
        for(int i=0; i < worldLength-1; i++) {
            queryFilter += "*";
        }
        FilterList filetList2 = FilterListBuilder.newInstance()
                .setQuick(queryFilter)
                .getFilterList();
        
        Collection<FilterList> aggregateQuery = new ArrayList<FilterList>();
        aggregateQuery.add(filterList);
        aggregateQuery.add(filetList2);
        
        double aggregateQueryValue = distributedQuery.query(aggregateQuery);
        
        logger.info("Query: " + filterList + ", " + filetList2 + " => " + aggregateQueryValue);
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
