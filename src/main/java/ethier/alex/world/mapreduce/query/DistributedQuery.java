/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.query.Query;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**

 @author alex
 */
public class DistributedQuery implements Query {
    
    private Configuration conf;
    private String[] args;
    private String worldSize;
    private String basePath;
    private int[] radices;
    
    public DistributedQuery(String myBasePath, int[] myRadices, Configuration myConf, String[] myArgs) throws Exception {
        conf = myConf;
        args = myArgs;
        basePath = myBasePath;
        radices = myRadices;
        
        WorldSizeRunner worldSizeRunner = new WorldSizeRunner(myBasePath + "/completed", "/world/default", myRadices);
        int ret = ToolRunner.run(conf, worldSizeRunner, args);
        if(ret != 0) {
            throw new RuntimeException("Unable to instantiate Distributed Query.");
        }
        
        worldSize = worldSizeRunner.getWorldSize();
    }

    @Override
    public BigDecimal getWorldSize() {
        return new BigDecimal(worldSize);
    }

    @Override
    public double query(FilterList filter) {
        Collection<FilterList> filters = new ArrayList<FilterList>();
        filters.add(filter);
        return query(filters);
    }

    @Override
    public double query(Collection<FilterList> filters) {
        try {
            QueryRunner queryRunner = new QueryRunner(worldSize, filters, basePath + "/completed", radices, basePath);
            int ret = ToolRunner.run(conf, queryRunner, args);
            if(ret != 0) {
                throw new RuntimeException("Query Runner Failed.");
            }
            
            BigDecimal probabilityOutput = new BigDecimal(queryRunner.getProbabilityOutput());
            return probabilityOutput.doubleValue();
        } catch (Exception ex) {
            throw new RuntimeException("Query failed, caused by: " + ExceptionUtils.getFullStackTrace(ex));
        }
    }
}
