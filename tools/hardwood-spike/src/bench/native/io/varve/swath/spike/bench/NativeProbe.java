/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;
import io.varve.swath.spike.core.HardwoodCore;
import io.varve.swath.spike.core.Fixture;
import io.varve.swath.spike.core.EncodingProfiles;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** Native feasibility smoke only; it does not attempt to compile swath. */
public final class NativeProbe {
    public static void main(String[] args) throws Exception {
        Path path=Path.of(args[0]); CompressionCodec codec=CompressionCodec.valueOf(args[1]);
        String profile=args.length>2?args[2]:"auto";
        var rows=Fixture.rows("canonical");
        try(var w=ParquetFileWriter.create(OutputFile.of(path),HardwoodCore.schema(),EncodingProfiles.apply(WriterConfig.builder(),profile).codec(codec).rowGroupTargetRows(257).build())) {
            for(Object[] row:rows)HardwoodCore.row(w,row);
        }
        int n=0;
        try(var f=ParquetFileReader.open(InputFile.of(path));var rr=f.rowReader()) {
            while(rr.hasNext()) {rr.next();Object[] a=new Object[13];
                for(int j=0;j<13;j++)if(!rr.isNull(j)) {
                    if(j==1||j==2)a[j]=rr.getLong(j);
                    else if(j==6||j==7)a[j]=rr.getBoolean(j);
                    else a[j]=new String(rr.getBinary(j),StandardCharsets.UTF_8);
                }
                Fixture.equal(a,rows.get(n),n++);
            }
        }
        Fixture.check(n==rows.size(),"native missing rows");
        System.out.println(Fixture.json(java.util.Map.of("check","native_roundtrip","codec",codec,"rows",n,"status","PASS","encoding_profile",profile)));
    }
}
