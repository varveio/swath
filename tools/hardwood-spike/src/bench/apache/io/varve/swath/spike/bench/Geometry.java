/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.bench;

import java.nio.file.Path;
import java.util.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

public final class Geometry {
    private static final class Column {
        long compressed,uncompressed,values,pages;
        final Set<String> encodings=new TreeSet<>(),codecs=new TreeSet<>();
        Map<String,Object> json(){return Map.of("compressed_bytes",compressed,"uncompressed_bytes",uncompressed,
                "values",values,"pages",pages,"encodings",encodings,"codecs",codecs);}
    }
    public static void main(String[] args) throws Exception {
        for(String path:args)try(var r=ParquetFileReader.open(new LocalInputFile(Path.of(path)))) {
            var groups=r.getFooter().getBlocks();
            long pages=0,rows=0;var sizes=new ArrayList<Long>();var counts=new ArrayList<Long>();
            var encodings=new TreeSet<String>();var codecs=new TreeSet<String>();
            var columns=new TreeMap<String,Column>();
            for(var group:groups) {
                sizes.add(group.getTotalByteSize());counts.add(group.getRowCount());rows+=group.getRowCount();
                for(var c:group.getColumns()) {
                    String name=c.getPath().toDotString();Column column=columns.computeIfAbsent(name,k->new Column());
                    column.compressed+=c.getTotalSize();column.uncompressed+=c.getTotalUncompressedSize();column.values+=c.getValueCount();
                    c.getEncodings().forEach(e->column.encodings.add(e.name()));column.codecs.add(c.getCodec().name());
                    encodings.add(c.getPath()+":"+c.getEncodings());codecs.add(c.getCodec().name());
                    var index=r.readOffsetIndex(c);if(index!=null){pages+=index.getPageCount();column.pages+=index.getPageCount();}
                }
            }
            var out=new LinkedHashMap<String,Object>();out.put("path",path);out.put("logical_rows",rows);out.put("row_groups",groups.size());
            out.put("row_group_rows",counts);out.put("row_group_uncompressed_bytes",sizes);out.put("all_column_pages",pages);
            out.put("codecs",codecs);out.put("encodings",encodings);
            var cols=new TreeMap<String,Object>();columns.forEach((name,column)->cols.put(name,column.json()));out.put("columns",cols);
            System.out.println(json(out));
        }
    }
    private static String json(Object value){
        if(value instanceof Map<?,?> map){var j=new StringJoiner(",","{","}");map.forEach((k,v)->j.add(json(k.toString())+":"+json(v)));return j.toString();}
        if(value instanceof Iterable<?> list){var j=new StringJoiner(",","[","]");for(Object v:list)j.add(json(v));return j.toString();}
        if(value instanceof Number||value instanceof Boolean)return value.toString();
        String s=value.toString();var b=new StringBuilder("\"");for(char c:s.toCharArray()){
            switch(c){case '"'->b.append("\\\"");case '\\'->b.append("\\\\");default->{if(c<32)b.append(String.format(Locale.ROOT,"\\u%04x",(int)c));else b.append(c);}}
        }return b.append('"').toString();
    }
}
