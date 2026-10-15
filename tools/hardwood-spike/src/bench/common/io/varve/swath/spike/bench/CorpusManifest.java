/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Expected values and ordering are established without opening any candidate-produced file. */
public final class CorpusManifest {
    public static void main(String[] args)throws Exception {
        String corpus=args[0];long rows=Long.parseLong(args[1]);var digest=new Corpus.Digest();
        byte[] previous=null;boolean sorted=true;int maxKey=0;long minSize=Long.MAX_VALUE,maxSize=0;
        long minTime=Long.MAX_VALUE,maxTime=Long.MIN_VALUE;long[] nulls=new long[13];
        for(long i=0;i<rows;i++) {
            Object[] row=Corpus.row(corpus,i);digest.row(row);
            byte[] key=((String)row[0]).getBytes(StandardCharsets.UTF_8);maxKey=Math.max(maxKey,key.length);
            if(previous!=null&&Arrays.compareUnsigned(previous,key)>=0)sorted=false;previous=key;
            long size=(Long)row[1],time=(Long)row[2];minSize=Math.min(minSize,size);maxSize=Math.max(maxSize,size);minTime=Math.min(minTime,time);maxTime=Math.max(maxTime,time);
            for(int j=0;j<13;j++)if(row[j]==null)nulls[j]++;
        }
        if(corpus.equals("irregular")&&!sorted)throw new AssertionError("Irregular keys must be strictly unsigned-UTF8 sorted");
        if(maxKey>1024)throw new AssertionError("Key exceeds S3 limit: "+maxKey);
        System.out.printf("{\"corpus\":\"%s\",\"rows\":%d,\"logical_sha256\":\"%s\",\"strict_unsigned_utf8_order\":%s,\"max_key_bytes\":%d,\"min_size\":%d,\"max_size\":%d,\"min_timestamp_micros\":%d,\"max_timestamp_micros\":%d,\"null_counts\":%s}%n",corpus,rows,digest.finish(),sorted,maxKey,minSize,maxSize,minTime,maxTime,Arrays.toString(nulls));
    }
}
