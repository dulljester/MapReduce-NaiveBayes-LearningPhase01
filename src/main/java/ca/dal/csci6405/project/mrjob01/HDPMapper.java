package ca.dal.csci6405.project.mrjob01;

/**
 * Created by serikzhan on 24/03/17.
 */
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HDPMapper extends Mapper<LongWritable,Text,PokerHandKey,IntWritable> {
    private static int M = 5, WIDTH = 6;
    private long readPokerHand( Scanner scan ) {
        long u = 0;
        int []card = new int[M];
        int i,j,k,m = 0;
        for ( i = 0; i < M; ++i ) {
            // 0 <= suit <= 3 ==> 2 bits
            // 1 <= rank <= 13 ==> 4 bits
            int suit = scan.nextInt()-1, rank = scan.nextInt();
            MyUtils.myAssert(0<=suit&&suit<=3);
            MyUtils.myAssert(1<=rank&&rank<=13);
            card[m++] = suit|(rank<<2);
        }
        //bubble sort the cards in descending order: this way, we "normalize" the hand
        for ( boolean flag = true; flag; )
            for ( flag = false, i = 0; i < M-1; ++i )
                if ( card[i] < card[i+1] ) {
                    k = card[i];
                    card[i] = card[i+1];
                    card[i+1] = k;
                    flag = true ;
                }
        for ( i = 0; i < M; u |= (((long)card[i]) << (i*WIDTH)), ++i ) ;
        long kk = scan.nextLong()+1;
        MyUtils.myAssert(1 <= kk && kk <= 10);
        return u|(kk<<(i*WIDTH));
    }
    private final IntWritable ONE = new IntWritable(1);
    // this map emits three types of keys: (term,1), (class,1), ((class,term),1)
    public void map( LongWritable key, Text value, Context con ) throws IOException, InterruptedException {
        String txt = value.toString();
        for ( String line : txt.split("\n") ) {
            long u = readPokerHand(new Scanner(line));
            for ( int i = 0; i < M; ++i ) {
                long v = u & (MyUtils.MASK(WIDTH)<<(i*WIDTH));
                con.write(new PokerHandKey(v),ONE); // (term,1)
                con.write(new PokerHandKey(v|(u&(MyUtils.MASK(4)<<(M*WIDTH)))),ONE); // ((classLabel,term),1) composite key
            }
            con.write(new PokerHandKey(u&(MyUtils.MASK(4)<<(M*WIDTH))),ONE); // (classLabel,1)
        }
    }
}

