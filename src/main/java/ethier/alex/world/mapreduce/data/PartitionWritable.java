/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.data;

import ethier.alex.world.core.data.Partition;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class PartitionWritable implements Writable {

    private static Logger logger = Logger.getLogger(PartitionWritable.class);
    private Partition partition;
    int worldSize;

    public PartitionWritable() {
        // No arg constructor required for hadoop Writable.
    }

    public PartitionWritable(Partition myPartition) {
        partition = myPartition;
    }

    public Partition getPartition() {
        return partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        partition.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        partition = new Partition(in);
    }
}
