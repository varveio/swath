/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import io.varve.swath.model.*;
import io.varve.swath.output.parquet.*;
import java.nio.file.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.example.data.Group;

public final class ApacheBench {
    public static void main(String[] args) throws Exception {
        String mode=args[0];Path path=Path.of(args[1]);long rows=Long.parseLong(args[2]);
        long target=Long.parseLong(args[3]);
        try(var heap=new Corpus.Heap()) {
            long start=System.nanoTime();
            if(mode.equals("write")) {
                var spec=new ListEntryParquetWriters.TrackedSpec(target,ListEntryParquetWriters.PageLayout.served(20000),0,null);
                var w=ListEntryParquetWriters.buildTracked(path,new ListEntryWriteSupport(ParquetSchema.canonical()),spec);
                for(long i=0;i<rows;i++) {Object[] r=Corpus.row(i);
                    w.write(new ObjectEntry(KeyBytes.ofUtf8((String)r[0]),(Long)r[1],(Long)r[2],(String)r[3],(String)r[4],(String)r[5],Boolean.TRUE.equals(r[6]),(String)r[8],(String)r[9],(String)r[10],(String)r[11]));
                }
                // Both writer measurements finalize the footer. The candidate's built-in output
                // does not fsync, so use close here and expose that mismatch in the report.
                w.writer().close();
            } else {
                long n=0;
                try(var reader=ParquetFileReader.open(new LocalInputFile(path))) {
                    var schema=reader.getFooter().getFileMetaData().getSchema();
                    var io=new ColumnIOFactory().getColumnIO(schema);
                    org.apache.parquet.column.page.PageReadStore pages;
                    while((pages=reader.readNextRowGroup())!=null)try(var group=pages) {
                    var rr=io.getRecordReader(group,new GroupRecordConverter(schema));
                    for(long k=0;k<group.getRowCount();k++) {Group g=rr.read();Object[] a=new Object[13];
                        for(int j=0;j<13;j++)if(g.getFieldRepetitionCount(j)>0) {
                            if(j==1||j==2)a[j]=g.getLong(j,0);
                            else if(j==6||j==7)a[j]=g.getBoolean(j,0);
                            else a[j]=g.getBinary(j,0).toStringUsingUTF8();
                        }
                        Corpus.equal(a,n++);
                    }}
                }
                if(n!=rows)throw new AssertionError("row count="+n+" expected="+rows);
            }
            Corpus.report("apache",mode,rows,start,Files.size(path),heap,target);
        }
    }
}
