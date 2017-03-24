package ca.dal.csci6405.project.mrjob01;

/**
 * Created by serikzhan on 24/03/17.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.util.*;

public class HDPReducer extends Reducer<PokerHandKey,IntWritable,PokerHandKey,IntWritable> {
    public void reduce( PokerHandKey key, Iterable<IntWritable> values, Context context )
            throws IOException, InterruptedException {
        int cnt = 0;
        for ( Iterator<IntWritable> it = values.iterator(); it.hasNext(); cnt += Integer.parseInt(it.next().toString()) );
        context.write(key,new IntWritable(cnt));
    }
}

