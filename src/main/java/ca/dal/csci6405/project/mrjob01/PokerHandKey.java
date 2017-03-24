package ca.dal.csci6405.project.mrjob01;
/**
 * Created by sj on 23/03/17.
 */

import java.io.*;
import org.apache.hadoop.io.WritableComparable;

public class PokerHandKey implements WritableComparable<PokerHandKey> {
    /*
     * S1: suit of card 1, C1: rank of card 1, and so on
     * 0 <= S1 <= 3, => 2 bits \
     * 1 <= C1 <= 13 => 4 bits / => 6 bits per card
     * Thus, 30 bits per hand, plus 4 bits for class
     * Thus, 34 bits per instance => gotta encode in Long
     * We have 34 bits at our disposal. If the number stored at bits 31-34 is not null, then this key
     * also contains class info
     * Otherwise, a 6-bit block contains a value of the corresponding feature
     */
    private long u;

    @Override
    public boolean equals( Object obj ) {
        if ( !(obj instanceof PokerHandKey) )
            return false ;
        PokerHandKey p = (PokerHandKey)obj;
        return p.u == u;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(u);
    }

    public PokerHandKey() {} //for some reason, they need an empty constructor, too

    public PokerHandKey( long u ) {
        this.u = u;
    }

    @Override
    public String toString() {
        return u+"";
    }
    @Override
    public int compareTo(PokerHandKey o) {
        if ( u == o.u ) return 0;
        return u<o.u?-1:1;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(u);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        u = dataInput.readLong();
    }
}

