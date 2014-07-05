/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.addon.ElementListBuilder;
import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.core.data.ElementState;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**

 @author alex
 */
public class ElementListWritable implements Writable {

    private ElementList elementList;
    private int worldSize;
    
    public ElementListWritable() {
        
    }

    public ElementListWritable(ElementList myElementList) {
        elementList = myElementList;
    }
    
    public ElementList getElementList() {
        return elementList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.writeWorldSize(out);
        this.writeElements(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.readWorldSize(in);
        elementList = this.readElementList(in);    
    }

    private void writeWorldSize(DataOutput out) throws IOException {
        worldSize = elementList.getLength();
        out.writeInt(worldSize);
    }

    private void readWorldSize(DataInput in) throws IOException {
        worldSize = in.readInt();
    }

    private void writeElements(DataOutput out) throws IOException {
        int[] ordinals = elementList.getOrdinals();
        for (int i = 0; i < ordinals.length; i++) {
            out.writeInt(ordinals[i]);
        }

        ElementState[] elementStates = elementList.getElementStates();
        for (int i = 0; i < elementStates.length; i++) {
            out.writeUTF(elementStates[i].name());
        }
    }

    private ElementList readElementList(DataInput in) throws IOException {
        ElementListBuilder elementListBuilder = ElementListBuilder.newInstance();

        int[] ordinals = new int[worldSize];
        for (int i = 0; i < worldSize; i++) {
            ordinals[i] = in.readInt();
        }

        ElementState[] elementStates = new ElementState[worldSize];
        for (int i = 0; i < worldSize; i++) {
            elementStates[i] = ElementState.valueOf(in.readUTF());
        }

        elementListBuilder.setOrdinals(ordinals);
        elementListBuilder.setStates(elementStates);

        return elementListBuilder.getElementList();
    }
}
