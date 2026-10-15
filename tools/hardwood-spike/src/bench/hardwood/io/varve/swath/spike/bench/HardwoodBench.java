/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.HardwoodContext;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;
import io.varve.swath.spike.core.HardwoodCore;
import io.varve.swath.spike.core.EncodingProfiles;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.security.MessageDigest;

public final class HardwoodBench {
    public static void main(String[] args) throws Exception {
        String mode=args[0]; Path path=Path.of(args[1]); long rows=Long.parseLong(args[2]);
        long target=Long.parseLong(args[3]);
        long targetRows=args.length>4?Long.parseLong(args[4]):Long.MAX_VALUE;
        String corpus=args.length>5?args[5]:"legacy",profile=args.length>6?args[6]:"auto";
        try(var heap=new Corpus.Heap()) {
            long start=System.nanoTime();
            var digest=new Corpus.Digest(corpus.equals("irregular"));
            if(mode.equals("write")) {
                var config=EncodingProfiles.apply(WriterConfig.builder(),profile).codec(CompressionCodec.ZSTD).pageTargetBytes(1024*1024)
                        .pageTargetRows(20000).rowGroupTargetRows(targetRows)
                        .rowGroupBufferTargetBytes(target).statisticsTruncationLength(1024).build();
                try(var w=ParquetFileWriter.create(new DigestOutput(OutputFile.of(path)), HardwoodCore.schema(),config)) {
                    for(long i=0;i<rows;i++){Object[] row=Corpus.row(corpus,i);digest.row(row);HardwoodCore.row(w,row);}
                }
            } else {
                long n=0;
                try(var context=HardwoodContext.create(mode.equals("read-single")?1:Runtime.getRuntime().availableProcessors());
                        var f=ParquetFileReader.open(InputFile.of(path),context); var rr=f.rowReader()) {
                    while(rr.hasNext()) {rr.next();Object[] a=new Object[13];
                        for(int j=0;j<13;j++)if(!rr.isNull(j)) {
                            if(j==1||j==2)a[j]=rr.getLong(j);
                            else if(j==6||j==7)a[j]=rr.getBoolean(j);
                            else a[j]=new String(rr.getBinary(j),StandardCharsets.UTF_8);
                        }
                        Corpus.equal(a,corpus,n++);digest.row(a);
                    }
                }
                if(n!=rows)throw new AssertionError("row count="+n+" expected="+rows);
            }
            Corpus.report("hardwood",mode,rows,start,Files.size(path),heap,target,corpus,profile,digest.finish());
        }
    }
    /** Same streaming MD5 work as the current swath physical writer, without Hadoop. */
    private static final class DigestOutput implements OutputFile {
        private final OutputFile delegate;
        private final MessageDigest digest;
        private long digestNanos;
        DigestOutput(OutputFile delegate) throws Exception {this.delegate=delegate;digest=MessageDigest.getInstance("MD5");}
        public void create()throws IOException{delegate.create();}
        public long position(){return delegate.position();}
        public void write(ByteBuffer bytes)throws IOException{
            ByteBuffer snapshot=bytes.duplicate();delegate.write(bytes);
            long start=System.nanoTime();digest.update(snapshot);digestNanos+=System.nanoTime()-start;
        }
        public void close()throws IOException{delegate.close();digest.digest();}
        public void discard()throws IOException{delegate.discard();}
    }
}
