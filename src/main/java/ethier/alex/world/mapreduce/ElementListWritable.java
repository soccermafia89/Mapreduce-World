/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.core.data.ElementList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**

 @author alex
 */

// Wraps the internal Writable implementation in the hadoop writable implementation.
// This was done to avoid adding hadoop dependencies to the core package while providing core datatypes
// With a way to be serialized into a bytes.
public class ElementListWritable implements Writable {

    private ElementList elementList;
//    private int worldSize;
    
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
        elementList.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        elementList = new ElementList(in);
    }
}
