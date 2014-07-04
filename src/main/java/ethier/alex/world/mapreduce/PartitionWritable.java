/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.addon.ElementListBuilder;
import ethier.alex.world.addon.FilterListBuilder;
import ethier.alex.world.addon.PartitionBuilder;
import ethier.alex.world.core.data.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.io.Writable;

/**

 @author alex
 */
public class PartitionWritable implements Writable {

    private Partition partition;
    int worldSize;

    public PartitionWritable(Partition myPartition) {
        partition = myPartition;
    }

    public Partition getPartition() {
        return partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        writeWorldSize(out);
        writeRadices(out);
        writeElements(out);
        writeFilters(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.readWorldSize(in);

        PartitionBuilder partitionBuilder = PartitionBuilder.newInstance();

        partitionBuilder.setRadices(readRadices(in));
        partitionBuilder.setElements(readElementList(in));
        partitionBuilder.addFilters(readFilters(in));

        partition = partitionBuilder.getPartition();
    }

    private void writeWorldSize(DataOutput out) throws IOException {
        worldSize = partition.getRadices().length;
        out.write(worldSize);
    }

    private void readWorldSize(DataInput in) throws IOException {
        worldSize = in.readInt();
    }

    private void writeRadices(DataOutput out) throws IOException {
        int[] radices = partition.getRadices();
        for (int i = 0; i < radices.length; i++) {
            out.write(radices[i]);
        }
    }

    private int[] readRadices(DataInput in) throws IOException {

        int[] radices = new int[worldSize];

        for (int i = 0; i < worldSize; i++) {
            radices[i] = in.readInt();
        }

        return radices;
    }

    private void writeElements(DataOutput out) throws IOException {
        
        ElementListWritable elementListWritable = new ElementListWritable(partition.getElements());
        elementListWritable.write(out);
//        int[] ordinals = partition.getElements().getOrdinals();
//        for (int i = 0; i < ordinals.length; i++) {
//            out.write(ordinals[i]);
//        }
//
//        ElementState[] elementStates = partition.getElements().getElementStates();
//        for (int i = 0; i < elementStates.length; i++) {
//            out.writeUTF(elementStates[i].name());
//        }
    }

    private ElementList readElementList(DataInput in) throws IOException {
        
        ElementListWritable elementListWritable = new ElementListWritable();
        elementListWritable.readFields(in);
        return elementListWritable.getElementList();
        
//        ElementListBuilder elementListBuilder = ElementListBuilder.newInstance();
//
//        int[] ordinals = new int[worldSize];
//        for (int i = 0; i < worldSize; i++) {
//            ordinals[i] = in.readInt();
//        }
//
//        ElementState[] elementStates = new ElementState[worldSize];
//        for (int i = 0; i < worldSize; i++) {
//            elementStates[i] = ElementState.valueOf(in.readUTF());
//        }
//
//        elementListBuilder.setOrdinals(ordinals);
//        elementListBuilder.setStates(elementStates);
//
//        return elementListBuilder.getElementList();
    }
    
    private void writeFilters(DataOutput out) throws IOException {
        int numFilters = partition.getFilters().size();
        out.write(numFilters);
        
        for(FilterList filter : partition.getFilters()) {
            int[] ordinals = filter.getOrdinals();
            
            for(int i=0; i < ordinals.length;i++) {
                out.write(ordinals[i]);
            }
            
            FilterState[] filterStates = filter.getFilterStates();
            for(int i=0; i < filterStates.length;i++) {
                out.writeUTF(filterStates[i].name());
            }
        }
    }

    private Collection<FilterList> readFilters(DataInput in) throws IOException {
        int numFilters = in.readInt();

        Collection<FilterList> filters = new ArrayList<FilterList>();
        for (int i = 0; i < numFilters; i++) {
            FilterListBuilder filterListBuilder = FilterListBuilder.newInstance();

            int[] ordinals = new int[worldSize];
            for (int j = 0; j < worldSize; j++) {
                ordinals[j] = in.readInt();
            }

            filterListBuilder.setOrdinals(ordinals);

            FilterState[] filterStates = new FilterState[worldSize];
            for (int j = 0; j < worldSize; j++) {
                filterStates[j] = FilterState.valueOf(in.readUTF());
            }

            filterListBuilder.setFilterStates(filterStates);

            filters.add(filterListBuilder.getFilterList());
        }

        return filters;
    }
}
