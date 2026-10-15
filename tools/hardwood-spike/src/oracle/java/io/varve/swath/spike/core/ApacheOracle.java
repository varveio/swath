/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.core;

import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.ListEntryWriteSupport;
import io.varve.swath.output.parquet.ListEntryParquetWriters;
import io.varve.swath.model.*;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.*;
import java.nio.file.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.*;
import org.apache.parquet.example.data.simple.*;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.filter2.columnindex.*;
import org.apache.parquet.io.*;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/** Independent Apache decoding + production swath index/range entrypoints. No candidate linkage. */
public final class ApacheOracle {
    private static String expectedProfile;
    static void write(Path path,String kind)throws Exception {
        var conf=new Configuration(false);conf.setInt("parquet.compression.codec.zstd.level",3);
        try(var w=ExampleParquetWriter.builder(new LocalOutputFile(path)).withConf(conf).withType(ParquetSchema.canonical())
            .withExtraMetaData(Fixture.STAMP).withCompressionCodec(CompressionCodecName.ZSTD).withDictionaryEncoding(true)
            .withRowGroupSize(1024*1024).withPageSize(16*1024).withPageRowCountLimit(128).withColumnIndexTruncateLength(1024).build()) {
            for(Object[] row:Fixture.rows(kind)){
                Group g=new SimpleGroup(ParquetSchema.canonical());
                for(int i=0;i<13;i++) {Object v=row[i];if(v==null)continue;
                    if(i==1||i==2)g.add(i,(Long)v);else if(i==6||i==7)g.add(i,(Boolean)v);else g.add(i,Binary.fromConstantByteArray(Fixture.bytes((String)v)));
                }w.write(g);
            }
        }
        System.out.println(Fixture.json(Map.of("check","apache_write","status","PASS","kind",kind,"bytes",Files.size(path))));
    }
    static void writeProduction(Path path)throws Exception {
        var tracked=ListEntryParquetWriters.buildTracked(path,new ListEntryWriteSupport(ParquetSchema.canonical()),new ListEntryParquetWriters.TrackedSpec(1024*1024,ListEntryParquetWriters.PageLayout.served(128),0,null));
        boolean closed=false;try{for(var row:Fixture.rows("production"))tracked.write(model(row));tracked.closeWithDurability();closed=true;}finally{if(!closed)tracked.discard();}
        long pageV2=0;Set<String> encodings=new TreeSet<>();
        try(var f=ParquetFileReader.open(new LocalInputFile(path))){for(var block:f.getFooter().getBlocks())for(var c:block.getColumns())for(var encoding:c.getEncodings())encodings.add(encoding.name());PageReadStore pages;while((pages=f.readNextRowGroup())!=null){try(var p=pages){for(var column:ParquetSchema.canonical().getColumns()){var reader=p.getPageReader(column);org.apache.parquet.column.page.DataPage page;while((page=reader.readPage())!=null)if(page instanceof org.apache.parquet.column.page.DataPageV2)pageV2++;}}}}
        Fixture.check(pageV2>0,"actual production writer must emit DataPageV2");
        Fixture.check(encodings.stream().anyMatch(e->e.startsWith("DELTA")),"fixture must engage production delta/fallback encoding");
        System.out.println(Fixture.json(Map.of("check","actual_swath_production_write","status","PASS","rows",Fixture.rows("production").size(),"data_pages_v2",pageV2,"encodings",encodings.toString(),"bytes",tracked.bytes(),"md5",tracked.md5(),"writer","buildTracked + ListEntryWriteSupport + served page layout")));
    }
    static void read(Path path,String kind)throws Exception {read(path,kind,true);}
    static void read(Path path,String kind,boolean requireStamp)throws Exception {
        var expected=Fixture.rows(kind);var actualRows=new ArrayList<Object[]>();int n=0;int groups;int indexes=0;List<Long> groupRows=new ArrayList<>();
        try(var f=ParquetFileReader.open(new LocalInputFile(path))) {
            MessageType schema=f.getFooter().getFileMetaData().getSchema();Fixture.check(schema.equals(ParquetSchema.canonical()),"exact canonical schema: "+schema);
            if(requireStamp)for(var e:Fixture.STAMP.entrySet()) Fixture.check(e.getValue().equals(f.getFooter().getFileMetaData().getKeyValueMetaData().get(e.getKey())),"stamp "+e.getKey());
            if(requireStamp&&(path.getFileName().toString().startsWith("hardwood-")||path.getFileName().toString().equals("swath-owned.parquet")))Fixture.check("preserved".equals(f.getFooter().getFileMetaData().getKeyValueMetaData().get("spike.unrelated")),"late metadata merge preserves unrelated key");
            groups=f.getFooter().getBlocks().size();
            for(var block:f.getFooter().getBlocks()) {groupRows.add(block.getRowCount());for(var col:block.getColumns()) {Fixture.check(f.readOffsetIndex(col)!=null,"offset index "+col.getPath());Fixture.check(f.readColumnIndex(col)!=null,"column index "+col.getPath());if(col.getPath().toDotString().equals("key"))Fixture.check(f.readColumnIndex(col).getBoundaryOrder()==org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING,"key pages must declare ASCENDING incl single/equal pages");indexes++;}}
            var io=new ColumnIOFactory().getColumnIO(schema);PageReadStore pages;
            while((pages=f.readNextRowGroup())!=null){try(var page=pages){var rr=io.getRecordReader(page,new GroupRecordConverter(schema));for(long j=0;j<page.getRowCount();j++){
                Fixture.check(n<expected.size(),"extra row");Group g=rr.read();Object[] a=new Object[13];
                for(int i=0;i<13;i++)if(g.getFieldRepetitionCount(i)>0){if(i==1||i==2)a[i]=g.getLong(i,0);else if(i==6||i==7)a[i]=g.getBoolean(i,0);else {byte[] raw=g.getBinary(i,0).getBytes();Fixture.check(Arrays.equals(raw,Fixture.bytes((String)expected.get(n)[i])),"raw bytes row="+n+" column="+i);a[i]=g.getString(i,0);}}
                Fixture.equal(a,expected.get(n),n);actualRows.add(a);n++;
            }}}
        }
        Fixture.check(n==expected.size(),"row count");
        validateBounds(path,actualRows);
        if(expectedProfile!=null)validateEncodingProfile(path,kind,expectedProfile);
        if(requireStamp){var stamp=SortedParquetStamp.read(path).orElseThrow();Fixture.check(stamp.isKnownFormatVersion()&&stamp.fileFinal()&&stamp.fileIndex()==1,"production stamp detection");}
        var keys=SortedParquetIndex.firstKeysPerRowGroup(path);Fixture.check(keys.size()==groups,"index groups");int at=0;
        for(int i=0;i<keys.size();i++) {Fixture.check(Arrays.equals(keys.get(i).firstKey(),Fixture.bytes((String)expected.get(at)[0])),"actual row-group first key "+i);Fixture.check(keys.get(i).rowCount()==groupRows.get(i),"index group count");at+=Math.toIntExact(groupRows.get(i));}
        int swathRows=0;boolean modelCompatible=expected.stream().noneMatch(r->r[12].equals("OBJECT")&&r[1]==null);
        if(modelCompatible)try(var reader=new ParquetEntryReader(path)){while(reader.hasNext()){var entry=reader.next();Fixture.check(Arrays.equals(entry.key().rawUnsafe(),Fixture.bytes((String)expected.get(swathRows)[0])),"swath key bytes row="+swathRows);Fixture.check(entry.equals(model(expected.get(swathRows))),"full swath model equivalence row="+swathRows);swathRows++;}}
        if(modelCompatible)Fixture.check(swathRows==n,"swath entry count");
        else System.out.println(Fixture.json(Map.of("check","swath_entry_model_all_null_masks","status","UNSUPPORTED","reason","production ParquetEntryReader requires OBJECT size nonnull; schema permits null; exact low-level validation still covers every field")));
        System.out.println(Fixture.json(Map.of("check","apache_and_swath_read","status","PASS","kind",kind,"rows",n,"row_groups",groups,"group_rows",groupRows.toString(),"column_offset_index_pairs",indexes,"fixture_sha256",Fixture.digest(expected))));
        if(kind.startsWith("boundaries"))unicodeRanges(path,expected,groupRows);
        if(kind.equals("dense")) {bounded(path,expected,groupRows);randomRanges(path,expected,groupRows);}
        if(kind.equals("single")||kind.equals("equal")){try(var reader=new SortedParquetRowGroupReader(path)){assertObjects(reader.objectRange(0,Fixture.bytes("same-key"),true,null,expected.size(),true),expected,0,expected.size());}System.out.println(Fixture.json(Map.of("check","single_or_equal_key_page_boundary","status","PASS","kind",kind)));}
    }
    /** Independently pinned policy expectations; intentionally does not use EncodingProfiles. */
    static void validateEncodingProfile(Path path,String kind,String profile)throws Exception {
        Map<String,org.apache.parquet.column.Encoding> delta=Map.of("key",org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY,"etag",org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY,"version_id",org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY,"size",org.apache.parquet.column.Encoding.DELTA_BINARY_PACKED,"last_modified",org.apache.parquet.column.Encoding.DELTA_BINARY_PACKED);
        Fixture.check(profile.equals("auto")||profile.equals("delta"),"oracle known profile");
        Map<String,Long> pageEncodings=new TreeMap<>();int groups=0;List<Boolean> ownerDictionary=new ArrayList<>();
        try(var f=ParquetFileReader.open(new LocalInputFile(path))){
            for(var block:f.getFooter().getBlocks()){
                ownerDictionary.add(block.getColumns().get(9).getEncodings().contains(org.apache.parquet.column.Encoding.RLE_DICTIONARY));
                for(var chunk:block.getColumns())if(delta.containsKey(chunk.getPath().toDotString())){
                    if(profile.equals("delta")){Fixture.check(chunk.getEncodings().contains(delta.get(chunk.getPath().toDotString())),"forced delta footer encoding "+chunk.getPath());Fixture.check(!chunk.getEncodings().contains(org.apache.parquet.column.Encoding.RLE_DICTIONARY),"forced delta cannot carry dictionary");}
                    else Fixture.check(chunk.getEncodings().stream().noneMatch(e->e.name().startsWith("DELTA")),"AUTO control must remain PLAIN/dictionary");
                }
            }
            PageReadStore pages;while((pages=f.readNextRowGroup())!=null){groups++;try(var p=pages){for(var column:f.getFooter().getFileMetaData().getSchema().getColumns()){
                String name=column.getPath()[0];if(!delta.containsKey(name))continue;var reader=p.getPageReader(column);org.apache.parquet.column.page.DataPage page;
                while((page=reader.readPage())!=null){var encoding=page instanceof org.apache.parquet.column.page.DataPageV1 v1?v1.getValueEncoding():((org.apache.parquet.column.page.DataPageV2)page).getDataEncoding();
                    if(profile.equals("delta"))Fixture.check(encoding==delta.get(name),"actual forced delta page "+name+" got="+encoding);
                    else Fixture.check(encoding==org.apache.parquet.column.Encoding.PLAIN||encoding==org.apache.parquet.column.Encoding.RLE_DICTIONARY,"AUTO page encoding control");
                    pageEncodings.merge(name+":"+encoding.name(),1L,Long::sum);
                }
            }}}
        }
        if(kind.equals("dictionary-transitions"))Fixture.check(ownerDictionary.equals(List.of(true,false,true)),"AUTO unforced column must transition dictionary/plain/dictionary across groups: "+ownerDictionary);
        System.out.println(Fixture.json(Map.of("check","independent_actual_encoding_profile","status","PASS","profile",profile,"row_groups",groups,"target_page_encodings",pageEncodings.toString(),"owner_display_dictionary_by_group",ownerDictionary.toString(),"expectation_source","independent five-column encoding map")));
    }
    static int compareBound(java.nio.ByteBuffer encoded,Object value,int col){
        var bytes=encoded.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
        if(col==1||col==2)return Long.compare(bytes.getLong(),(Long)value);
        if(col==6||col==7)return Boolean.compare(bytes.get()!=0,(Boolean)value);
        byte[] raw=new byte[bytes.remaining()];bytes.get(raw);return Arrays.compareUnsigned(raw,Fixture.bytes((String)value));
    }
    /** Check every actual decoded non-null value against footer and physical-page index bounds. */
    static void validateBounds(Path path,List<Object[]> actual)throws Exception {
        long comparisons=0,pagesChecked=0;int base=0;Set<String> codecs=new TreeSet<>();
        try(var f=ParquetFileReader.open(new LocalInputFile(path))){
            for(var block:f.getFooter().getBlocks()){
                for(int col=0;col<13;col++){
                    var chunk=block.getColumns().get(col);codecs.add(chunk.getCodec().name());var stats=chunk.getStatistics();var index=f.readColumnIndex(chunk);var offsets=f.readOffsetIndex(chunk);Fixture.check(index.getNullPages().size()==offsets.getPageCount(),"column/offset page alignment");
                    long footerNulls=0;
                    for(int page=0;page<offsets.getPageCount();page++){
                        long first=offsets.getFirstRowIndex(page),last=offsets.getLastRowIndex(page,block.getRowCount()),pageNulls=0;pagesChecked++;
                        Fixture.check(first>=0&&last>=first&&last<block.getRowCount(),"offset row bounds");
                        for(long row=first;row<=last;row++){
                            Object value=actual.get(base+Math.toIntExact(row))[col];if(value==null){pageNulls++;footerNulls++;continue;}
                            Fixture.check(stats.hasNonNullValue(),"footer missing nonnull bounds");
                            Fixture.check(!index.getNullPages().get(page),"nonnull value cannot be in null page");
                            Fixture.check(compareBound(index.getMinValues().get(page),value,col)<=0&&compareBound(index.getMaxValues().get(page),value,col)>=0,"unsafe unsigned column-index bounds group-base="+base+" page="+page+" col="+col+" row="+row);
                            Fixture.check(compareBound(java.nio.ByteBuffer.wrap(stats.getMinBytes()),value,col)<=0&&compareBound(java.nio.ByteBuffer.wrap(stats.getMaxBytes()),value,col)>=0,"unsafe unsigned footer bounds group-base="+base+" col="+col+" row="+row);comparisons+=4;
                        }
                        Fixture.check(index.getNullPages().get(page)==(pageNulls==last-first+1),"page all-null flag");
                        if(index.getNullCounts()!=null)Fixture.check(index.getNullCounts().get(page)==pageNulls,"page null count");
                    }
                    if(stats.isNumNullsSet())Fixture.check(stats.getNumNulls()==footerNulls,"footer null count");
                }
                base+=Math.toIntExact(block.getRowCount());
            }
        }
        String filename=path.getFileName().toString();
        if(filename.startsWith("native-resource-")&&filename.endsWith("-ZSTD.parquet"))Fixture.check(codecs.equals(Set.of("ZSTD")),"native ZSTD codec metadata");
        if(filename.startsWith("native-resource-")&&filename.endsWith("-UNCOMPRESSED.parquet"))Fixture.check(codecs.equals(Set.of("UNCOMPRESSED")),"native uncompressed codec metadata");
        System.out.println(Fixture.json(Map.of("check","actual_value_footer_and_page_bounds","status","PASS","comparisons",comparisons,"physical_column_pages",pagesChecked,"codecs",codecs.toString(),"string_comparison","unsigned UTF-8","numeric_comparison","physical INT64 little-endian decoded to signed long")));
    }
    static void unicodeRanges(Path path,List<Object[]> expected,List<Long> groupRows)throws Exception {
        int queries=0,pruned=0,nonAscii=0;
        try(var f=ParquetFileReader.open(new LocalInputFile(path));var served=new SortedParquetRangeReader(path,1)){
            var projection=new MessageType("swath_listing",f.getFooter().getFileMetaData().getSchema().getType("key"));f.setRequestedSchema(projection);
            for(int start=0;start<expected.size()-3;start++){
                byte[] from=Fixture.bytes((String)expected.get(start)[0]);if(java.util.stream.IntStream.range(0,from.length).noneMatch(i->from[i]<0))continue;nonAscii++;
                int end=start+3;byte[] to=Fixture.bytes((String)expected.get(end)[0]);int group=0,base=0;while(base+groupRows.get(group)<=start){base+=Math.toIntExact(groupRows.get(group));group++;}
                var filter=FilterCompat.get(FilterApi.and(FilterApi.gtEq(FilterApi.binaryColumn("key"),Binary.fromConstantByteArray(from)),FilterApi.lt(FilterApi.binaryColumn("key"),Binary.fromConstantByteArray(to))));
                var ranges=ColumnIndexFilter.calculateRowRanges(filter,f.getColumnIndexStore(group),Set.of(org.apache.parquet.hadoop.metadata.ColumnPath.get("key")),groupRows.get(group));if(ranges.rowCount()<groupRows.get(group))pruned++;
                int firstInGroup=Math.max(start,base),endInGroup=Math.min(end,base+Math.toIntExact(groupRows.get(group))),matches=0;
                try(PageReadStore pages=f.readFilteredRowGroup(group,ranges)){
                    Fixture.check(pages.getRowCount()==ranges.rowCount(),"nonASCII actual indexed row count");var rr=new ColumnIOFactory().getColumnIO(projection).getRecordReader(pages,new GroupRecordConverter(projection));
                    for(long i=0;i<pages.getRowCount();i++){byte[] key=rr.read().getBinary(0,0).getBytes();if(Arrays.compareUnsigned(key,from)>=0&&Arrays.compareUnsigned(key,to)<0){Fixture.check(Arrays.equals(key,Fixture.bytes((String)expected.get(firstInGroup+matches)[0])),"nonASCII indexed exact result");matches++;}}
                }
                Fixture.check(matches==endInGroup-firstInGroup,"nonASCII indexed result count");
                assertObjects(served.range(group,from,true,to,4,true),expected,start,3);assertObjects(served.range(group,from,false,to,4,true),expected,start+1,2);queries+=3;
            }
        }
        Fixture.check(nonAscii>20&&pruned>0,"Unicode fixture must query multiple discriminatory pages and prune");
        System.out.println(Fixture.json(Map.of("check","nonascii_indexed_and_served_ranges","status","PASS","queries",queries,"nonascii_lower_bounds",nonAscii,"pruned_group_queries",pruned,"fixture_rows",expected.size(),"page_target_rows",2,"row_group_target_rows",7)));
    }
    static ListEntry model(Object[] r) {
        var key=KeyBytes.of(Fixture.bytes((String)r[0]));long micros=r[2]==null?0:(Long)r[2];boolean latest=Boolean.TRUE.equals(r[6]);
        return switch((String)r[12]) {
            case "COMMON_PREFIX" -> new CommonPrefixEntry(key);
            case "DELETE_MARKER" -> new DeleteMarkerEntry(key,(String)r[5],latest,micros,(String)r[8]);
            default -> new ObjectEntry(key,(Long)r[1],micros,(String)r[3],(String)r[4],(String)r[5],latest,(String)r[8],(String)r[9],(String)r[10],(String)r[11]);
        };
    }
    static void bounded(Path path,List<Object[]> expected,List<Long> groupRows)throws Exception {
        int group=groupRows.size()/2;int base=0;for(int i=0;i<group;i++)base+=Math.toIntExact(groupRows.get(i));
        int target=base+Math.toIntExact(groupRows.get(group))*3/4;int count=17;
        byte[] from=Fixture.bytes((String)expected.get(target)[0]), to=Fixture.bytes((String)expected.get(target+count)[0]);long selected;long total=groupRows.get(group);long pageRows;
        try(var f=ParquetFileReader.open(new LocalInputFile(path))){
            var block=f.getFooter().getBlocks().get(group);var key=block.getColumns().getFirst();var offsets=f.readOffsetIndex(key);Fixture.check(offsets.getPageCount()>2,"requires multi-page group");
            var projection=new MessageType("swath_listing",f.getFooter().getFileMetaData().getSchema().getType("key"));f.setRequestedSchema(projection);
            var filter=FilterCompat.get(FilterApi.and(FilterApi.gtEq(FilterApi.binaryColumn("key"),Binary.fromConstantByteArray(from)),FilterApi.lt(FilterApi.binaryColumn("key"),Binary.fromConstantByteArray(to))));
            var ranges=ColumnIndexFilter.calculateRowRanges(filter,f.getColumnIndexStore(group),Set.of(org.apache.parquet.hadoop.metadata.ColumnPath.get("key")),total);
            selected=ranges.rowCount();Fixture.check(selected>0&&selected<total/2,"page index must prune actual rows; selected="+selected+" total="+total);
            try(PageReadStore pages=f.readFilteredRowGroup(group,ranges)){pageRows=pages.getRowCount();Fixture.check(pageRows==selected,"filtered physical row count");var rr=new ColumnIOFactory().getColumnIO(projection).getRecordReader(pages,new GroupRecordConverter(projection));int matches=0;for(long i=0;i<pageRows;i++){byte[] k=rr.read().getBinary(0,0).getBytes();if(Arrays.compareUnsigned(k,from)>=0&&Arrays.compareUnsigned(k,to)<0){Fixture.check(Arrays.equals(k,Fixture.bytes((String)expected.get(target+matches)[0])),"bounded indexed key");matches++;}}Fixture.check(matches==count,"bounded indexed count");}
        }
        long decoded;
        try(var r=new SortedParquetRowGroupReader(path)) {
            try(var c=r.openKeyCursor(group,from,true,to)){Fixture.check(c.hasCurrent()&&Arrays.equals(c.currentKey(),from),"indexed cursor start");Fixture.check(c.position()==target-base,"indexed cursor absolute row position");decoded=c.decodedRows();Fixture.check(decoded<total/2,"indexed cursor bounded decode");}
            var rows=r.objectRange(group,from,true,to,count,true);assertObjects(rows,expected,target,count);
        }
        try(var r=new SortedParquetRangeReader(path,1)){assertObjects(r.range(group,from,true,to,count,true),expected,target,count);assertObjects(r.range(group,from,false,to,count,true),expected,target+1,count-1);}
        System.out.println(Fixture.json(Map.of("check","actual_indexed_bounded_read","status","PASS","row_group",group,"row_group_rows",total,"index_selected_rows",selected,"physical_read_rows",pageRows,"cursor_decoded_rows",decoded,"result_rows",count,"from_inclusive_and_exclusive",true)));
    }
    static void randomRanges(Path path,List<Object[]> expected,List<Long> groupRows)throws Exception {
        Random random=new Random(0x5A17);int checks=0;try(var r=new SortedParquetRangeReader(path,1)) {
            for(int i=0;i<80;i++){
                int start=i<16?Math.min(expected.size()-20,(i%8)*128+(i/8)*8192):random.nextInt(expected.size()-20);boolean inclusive=(i&1)==0;int group=0,base=0;while(base+groupRows.get(group)<=start){base+=Math.toIntExact(groupRows.get(group));group++;}
                int first=start+(inclusive?0:1);int count=1+random.nextInt(17);byte[] from=Fixture.bytes((String)expected.get(start)[0]);byte[] to=Fixture.bytes((String)expected.get(first+count)[0]);
                assertObjects(r.range(group,from,inclusive,to,count+1,true),expected,first,count);checks++;
            }
            Fixture.check(r.range(0,Fixture.bytes("zzzz"),true,null,10,true).isEmpty(),"beyond EOF empty");
            Fixture.check(r.range(0,null,true,null,0,true).isEmpty(),"zero limit empty");
        }
        System.out.println(Fixture.json(Map.of("check","random_and_page_boundary_ranges","status","PASS","queries",checks+2,"seed",0x5A17)));
    }
    static void assertObjects(List<SortedParquetRowGroupReader.ObjectRow> actual,List<Object[]> expected,int start,int count){
        Fixture.check(actual.size()==count,"range count actual="+actual.size()+" expected="+count);
        for(int i=0;i<count;i++){var a=actual.get(i);var e=expected.get(start+i);Fixture.check(Arrays.equals(a.key(),Fixture.bytes((String)e[0])),"range key "+i);Fixture.check(a.size()==(e[1]==null?0:(Long)e[1]),"range size");Fixture.check(a.lastModifiedEpochMicros()==(e[2]==null?0:(Long)e[2]),"range micros");Fixture.check(Objects.equals(a.etag(),e[3])&&Objects.equals(a.storageClass(),e[4])&&Objects.equals(a.ownerId(),e[8])&&Objects.equals(a.ownerDisplayName(),e[9])&&Objects.equals(a.checksumAlgorithm(),e[10])&&Objects.equals(a.checksumType(),e[11]),"range projected values");}
    }
    public static void main(String[] args)throws Exception{expectedProfile=args.length>3?args[3]:null;if(args[0].equals("write-production"))writeProduction(Path.of(args[1]));else if(args[0].equals("write"))write(Path.of(args[1]),args[2]);else if(args[0].equals("read"))read(Path.of(args[1]),args[2]);else if(args[0].equals("read-unstamped"))read(Path.of(args[1]),args[2],false);else throw new IllegalArgumentException(args[0]);}
}
