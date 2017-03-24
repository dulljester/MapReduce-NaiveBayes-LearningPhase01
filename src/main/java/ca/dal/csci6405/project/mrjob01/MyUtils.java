package ca.dal.csci6405.project.mrjob01;
/**
 * Created by serikzhan on 24/03/17.
 */

public class MyUtils {
     private final static int S = 20;
     public static long BIT( int k ) { return 1L<<k;}
     public static long MASK( int k ) { return BIT(k)-1L; }
     private static int []which = new int [1<<S];
     static {
         for (int i = 0; i < S; which[(int) BIT(i)] = i, ++i) ;
     }
     public static void myAssert(boolean b) {
         if ( !b ) {
             int trap = 1/0;
         }
     }
}
