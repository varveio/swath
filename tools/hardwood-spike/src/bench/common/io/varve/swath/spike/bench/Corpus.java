/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/** Streamed deterministic object listings; bounded state independent of corpus length. */
public final class Corpus {
    private static final String[] IRREGULAR_SUFFIXES={"\u0000","a\u0000","a","b","z","\u007f","\u0080","é","Ω","中","\ud7ff","\ue000","\uffff","\ud800\udc00","\udbff\udfff","\udbff\udfffz"};
    private Corpus() {}
    public static Object[] row(long i) {
        String key = "objects/" + Long.toHexString(i / 1000) + "/" + Long.toHexString(i)
                + (i % 257 == 0 ? "/中é/" + "x".repeat(900) : "/" + "key".repeat((int)(i % 23)));
        boolean version = i % 3 != 0;
        return new Object[] {key, i * 104729, 1_700_000_000_000_000L + i * 1001,
                Long.toHexString(i * 0x123456789abcdefL), i % 5 == 0 ? "GLACIER" : "STANDARD",
                version ? "v" + (i % 10007) : null, version ? i % 2 == 0 : null, false,
                i % 7 == 0 ? null : "owner-" + i % 11, i % 7 == 0 ? null : "display 中",
                i % 11 == 0 ? "CRC32C,SHA256" : null, i % 11 == 0 ? "FULL_OBJECT" : null, "OBJECT"};
    }
    public static Object[] row(String corpus, long i) {
        if(corpus.equals("legacy"))return row(i);
        if(!corpus.equals("irregular"))throw new IllegalArgumentException("Unknown corpus: "+corpus);
        // The zero-padded block and unsigned-UTF8-ordered suffix make this stream sorted.
        long h=mix(i+0x5eedc0deL), h2=mix(h);
        String key="inventory/tenant-"+decimal(i/100000,4)+"/objects/"+decimal(i/16,12)+"/"+IRREGULAR_SUFFIXES[(int)(i%16)]
                +"/"+Long.toUnsignedString(h,16)+(i%257==0?"/"+"x".repeat(900):"/payload.bin");
        long size=i%101==0?0:Math.floorMod(h,1L<<(10+(int)(i%22)));
        long timestamp=i%65537==0?-1_000_001L:1_680_000_000_000_000L+Math.floorMod(h2,365L*86400*1_000_000);
        boolean version=i%3!=0;
        return new Object[]{key,size,timestamp,hex(h)+hex(h2)+(i%11==0?"-3":""),
                switch((int)(i%7)){case 0->"GLACIER";case 1->"STANDARD_IA";default->"STANDARD";},
                version?"v-"+Long.toUnsignedString(mix(i),36):null,version?i%2==0:null,false,
                i%7==0?null:"owner-"+i%11,i%7==0?null:"display 中",
                i%11==0?"CRC32C,SHA256":null,i%11==0?"FULL_OBJECT":null,"OBJECT"};
    }
    private static String decimal(long n,int width){String s=Long.toString(n);return "0".repeat(Math.max(0,width-s.length()))+s;}
    private static String hex(long n){String s=Long.toHexString(n);return "0".repeat(16-s.length())+s;}
    private static long mix(long n){n=(n^(n>>>30))*0xbf58476d1ce4e5b9L;n=(n^(n>>>27))*0x94d049bb133111ebL;return n^(n>>>31);}
    public static void equal(Object[] got, long i) {
        equal(got,"legacy",i);
    }
    public static void equal(Object[] got, String corpus, long i) {
        Object[] expected = row(corpus,i);
        if (!java.util.Arrays.equals(got, expected)) throw new AssertionError("row mismatch " + i + " got=" + java.util.Arrays.toString(got));
    }
    /** Typed length-delimited stream hash, computed from decoded values on reads. */
    public static final class Digest {
        private final MessageDigest digest;
        private final byte[] scalar=new byte[8];
        private final boolean requireSorted;
        private byte[] previous;
        public Digest()throws Exception{this(false);}
        public Digest(boolean requireSorted)throws Exception{digest=MessageDigest.getInstance("SHA-256");this.requireSorted=requireSorted;}
        public void row(Object[] row){
            if(requireSorted){byte[] key=((String)row[0]).getBytes(StandardCharsets.UTF_8);if(previous!=null&&java.util.Arrays.compareUnsigned(previous,key)>=0)throw new AssertionError("Decoded keys not strictly unsigned UTF8 sorted");previous=key;}
            for(Object value:row){
            if(value==null){digest.update((byte)0);continue;}
            if(value instanceof Long n){digest.update((byte)1);long v=n;for(int j=7;j>=0;j--){scalar[j]=(byte)v;v>>>=8;}digest.update(scalar);}
            else if(value instanceof Boolean b){digest.update((byte)2);digest.update((byte)(b?1:0));}
            else{digest.update((byte)3);byte[] bytes=((String)value).getBytes(StandardCharsets.UTF_8);int length=bytes.length;for(int j=3;j>=0;j--)digest.update((byte)(length>>>(j*8)));digest.update(bytes);}
        }}
        public String finish(){return HexFormat.of().formatHex(digest.digest());}
    }
    public static final class Heap implements AutoCloseable {
        private final AtomicLong peak = new AtomicLong();
        private final Thread sampler;
        public Heap() {
            sampler = Thread.ofPlatform().daemon().start(() -> {
                while (!Thread.interrupted()) {
                    peak.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
                    try { Thread.sleep(10); } catch (InterruptedException e) { break; }
                }
            });
        }
        public long peak() { return peak.get(); }
        public void close() { sampler.interrupt(); }
    }
    public static void report(String engine, String mode, long rows, long start, long bytes, Heap heap, long target) {
        System.out.printf(java.util.Locale.ROOT,
                "{\"engine\":\"%s\",\"mode\":\"%s\",\"rows\":%d,\"operation_seconds\":%.6f,\"file_bytes\":%d,\"sampled_peak_heap_bytes\":%d,\"row_group_target_bytes\":%d}%n",
                engine, mode, rows, (System.nanoTime()-start)/1e9, bytes, heap.peak(), target);
    }
    public static void report(String engine,String mode,long rows,long start,long bytes,Heap heap,long target,String corpus,String profile,String hash){
        System.out.printf(java.util.Locale.ROOT,"{\"engine\":\"%s\",\"mode\":\"%s\",\"rows\":%d,\"operation_seconds\":%.6f,\"file_bytes\":%d,\"sampled_peak_heap_bytes\":%d,\"row_group_target_bytes\":%d,\"corpus\":\"%s\",\"encoding_profile\":\"%s\",\"logical_sha256\":\"%s\"}%n",engine,mode,rows,(System.nanoTime()-start)/1e9,bytes,heap.peak(),target,corpus,profile,hash);
    }
}
