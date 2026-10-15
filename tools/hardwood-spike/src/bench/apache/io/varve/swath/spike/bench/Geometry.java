/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import java.nio.file.Path;
import java.util.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

public final class Geometry {
    public static void main(String[] args) throws Exception {
        for(String path:args)try(var r=ParquetFileReader.open(new LocalInputFile(Path.of(path)))) {
            var groups=r.getFooter().getBlocks();
            long pages=0;var sizes=new ArrayList<Long>();var counts=new ArrayList<Long>();
            var encodings=new TreeSet<String>();var codecs=new TreeSet<String>();
            for(var group:groups) {
                sizes.add(group.getTotalByteSize());counts.add(group.getRowCount());
                for(var c:group.getColumns()) {
                    encodings.add(c.getPath()+":"+c.getEncodings());codecs.add(c.getCodec().name());
                    var index=r.readOffsetIndex(c);if(index!=null)pages+=index.getPageCount();
                }
            }
            System.out.printf("{\"path\":\"%s\",\"row_groups\":%d,\"row_group_rows\":%s,\"row_group_uncompressed_bytes\":%s,\"all_column_pages\":%d,\"codecs\":\"%s\",\"encodings\":\"%s\"}%n",
                    path,groups.size(),counts,sizes,pages,codecs,encodings);
        }
    }
}
