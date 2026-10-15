/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.core;

import dev.hardwood.*;
import dev.hardwood.metadata.*;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.reader.RowReader;
import dev.hardwood.writer.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** Candidate process: intentionally has no Apache Parquet or Hadoop linkage. */
public final class HardwoodCore {
    private static String activeProfile="auto";
    public static FileSchema schema() {
        var b=FileSchema.builder("swath_listing");
        for(int i=0;i<13;i++) {
            var rep=(i==0||i==7||i==12)?RepetitionType.REQUIRED:RepetitionType.OPTIONAL;
            if(i==1)b.addColumn(Fixture.NAMES[i],PhysicalType.INT64,rep);
            else if(i==2)b.addColumn(Fixture.NAMES[i],PhysicalType.INT64,rep,LogicalType.timestamp(true,LogicalType.TimeUnit.MICROS));
            else if(i==6||i==7)b.addColumn(Fixture.NAMES[i],PhysicalType.BOOLEAN,rep);
            else b.addColumn(Fixture.NAMES[i],PhysicalType.BYTE_ARRAY,rep,LogicalType.string());
        }
        return b.build();
    }
    public static WriterConfig config(long rows) {return config(rows,activeProfile);}
    public static WriterConfig config(long rows,String profile) {
        return EncodingProfiles.apply(WriterConfig.builder(),profile).pageTargetRows(128).pageTargetBytes(16*1024).rowGroupTargetRows(rows)
            .rowGroupBufferTargetBytes(8L*1024*1024).statisticsTruncationLength(1024).codec(CompressionCodec.ZSTD).build();
    }
    public static void row(ParquetFileWriter w, Object[] row) throws IOException {
        w.rowWriter().writeRow(b->{for(int i=0;i<13;i++) {
            Object v=row[i]; if(v==null)b.setNull(i);
            else if(i==1||i==2)b.setLong(i,(Long)v); // raw epoch-microseconds: documented physical setter preserves logical annotation
            else if(i==6||i==7)b.setBoolean(i,(Boolean)v);
            else b.setBinary(i,Fixture.bytes((String)v));
        }});
    }
    public static void write(Path path,String kind) throws Exception {
        var cfg=kind.startsWith("boundaries")?EncodingProfiles.apply(WriterConfig.builder(),activeProfile).pageTargetRows(2).pageTargetBytes(16384).rowGroupTargetRows(7).rowGroupBufferTargetBytes(8L*1024*1024).statisticsTruncationLength(kind.endsWith("truncated")?64:1024).codec(CompressionCodec.ZSTD).build():config(kind.equals("dense")?8192:kind.equals("equal")?10000:kind.equals("dictionary-transitions")?1024:257);
        try(var w=ParquetFileWriter.create(OutputFile.of(path),schema(),cfg)) {
            // Stamp last to prove metadata remains mutable through buffered row writes.
            for(Object[] r:Fixture.rows(kind))row(w,r);
            w.keyValueMetadata("spike.unrelated","preserved");w.keyValueMetadata("swath.sort.file_final","false");
            w.keyValueMetadata(Fixture.STAMP);
        }
        System.out.println(Fixture.json(Map.of("check","hardwood_write","status","PASS","kind",kind,"bytes",Files.size(path),"fixture_sha256",Fixture.digest(Fixture.rows(kind)))));
    }
    public static void read(Path path,String kind) throws Exception {
        var expected=Fixture.rows(kind); int n=0;
        try(var f=dev.hardwood.reader.ParquetFileReader.open(InputFile.of(path));RowReader rr=f.rowReader()) {
            Fixture.check(f.getFileSchema().getColumnCount()==13,"13 columns");
            while(rr.hasNext()) {rr.next();Fixture.check(n<expected.size(),"extra row");Object[] a=new Object[13];
                for(int i=0;i<13;i++)if(!rr.isNull(i)) {
                    if(i==1||i==2)a[i]=rr.getLong(i);
                    else if(i==6||i==7)a[i]=rr.getBoolean(i);
                    else {byte[] bytes=rr.getBinary(i);Fixture.check(Arrays.equals(bytes,Fixture.bytes((String)expected.get(n)[i])),"raw UTF8 row="+n+" col="+i);a[i]=new String(bytes,java.nio.charset.StandardCharsets.UTF_8);}
                }
                Fixture.equal(a,expected.get(n),n++);
            }
        }
        Fixture.check(n==expected.size(),"missing rows");
        System.out.println(Fixture.json(Map.of("check","hardwood_read","status","PASS","kind",kind,"rows",n,"fixture_sha256",Fixture.digest(expected))));
    }
    public static void geometry(Path dir) throws Exception {
        Path p=dir.resolve("manual-rowgroups.parquet");
        try(var w=ParquetFileWriter.create(OutputFile.of(p),schema(),config(100000))) {
            w.endRowGroup(); // empty cut must emit no empty row group
            var fixture=Fixture.rows("canonical");
            for(int i=0;i<1000;i++) {row(w,fixture.get(i));if(i==2||i==302||i==999){w.endRowGroup();w.endRowGroup();}}
        }
        try(var f=dev.hardwood.reader.ParquetFileReader.open(InputFile.of(p))) {
            var groups=f.getFileMetaData().rowGroups();
            Fixture.check(groups.size()==3,"manual cuts should be exactly 3 groups");
            Fixture.check(groups.get(0).numRows()==3 && groups.get(1).numRows()==300 && groups.get(2).numRows()==697,"manual row geometry");
            System.out.println(Fixture.json(Map.of("check","endRowGroup","status","PASS","row_groups","3,300,697","empty_cut_groups",0)));
        }
    }
    public static void byteGeometry(Path dir)throws Exception {
        Path p=dir.resolve("byte-target-rowgroups.parquet");var fixture=Fixture.rows("semantic");
        var cfg=EncodingProfiles.apply(WriterConfig.builder(),activeProfile).pageTargetRows(32).pageTargetBytes(8192).rowGroupBufferTargetBytes(16384).rowGroupTargetRows(1_000_000).codec(CompressionCodec.ZSTD).build();
        try(var w=ParquetFileWriter.create(OutputFile.of(p),schema(),cfg)){
            for(int i=0;i<512;i++){var r=fixture.get(i).clone();r[0]=String.format(Locale.ROOT,"bytes/%08d",i)+"x".repeat(1010);row(w,r);}
        }
        try(var f=dev.hardwood.reader.ParquetFileReader.open(InputFile.of(p))){var gs=f.getFileMetaData().rowGroups();Fixture.check(gs.size()>1,"byte budget engages before huge row target");long rows=0,max=0;for(var g:gs){rows+=g.numRows();max=Math.max(max,g.numRows());}Fixture.check(rows==512&&max<512,"byte target geometry rows");System.out.println(Fixture.json(Map.of("check","byte_target_rowgroup_cut","status","PASS","buffer_target_bytes",16384,"row_target",1000000,"row_groups",gs.size(),"max_group_rows",max,"rows",rows,"hard_rss_bound_proven",false)));}
    }
    /** Retain a bounded repro for the release's convenience conversion lower-bound overflow. */
    public static void timestampConvenienceBoundary(Path dir)throws Exception {
        long micros=Long.MIN_VALUE;Instant instant=Instant.ofEpochSecond(Math.floorDiv(micros,1_000_000),Math.floorMod(micros,1_000_000)*1000);
        Fixture.check(java.math.BigInteger.valueOf(instant.getEpochSecond()).multiply(java.math.BigInteger.valueOf(1_000_000)).add(java.math.BigInteger.valueOf(instant.getNano()/1000)).equals(java.math.BigInteger.valueOf(micros)),"mathematically exact INT64 microsecond boundary");
        Path path=dir.resolve("timestamp-convenience-lower-bound.parquet");var writer=ParquetFileWriter.create(OutputFile.of(path),schema(),config(128));boolean rejected=false;String message="";
        try{writer.rowWriter().writeRow(b->b.setString("key","timestamp-boundary").setBoolean("is_delete_marker",false).setTimestamp("last_modified",instant).setString("row_type","OBJECT"));}
        catch(IllegalArgumentException e){Fixture.check(e.getCause() instanceof ArithmeticException&&e.getMessage().contains("outside the range"),"unexpected convenience rejection");rejected=true;message=e.getMessage();}
        finally{writer.close();}
        Fixture.check(rejected&&!Files.exists(path)&&!Files.exists(Path.of(path+".hardwood-tmp")),"known valid-range convenience conversion rejected and discarded");
        System.out.println(Fixture.json(Map.of("check","timestamp_convenience_lower_bound","status","UNSUPPORTED","profile",activeProfile,"epoch_micros",micros,"instant",instant.toString(),"reason",message,"adapter_policy","setLong raw physical INT64; logical TIMESTAMP(MICROS,UTC) schema retained","scope","convenience conversion only; raw delta/adversarial values independently checked")));
    }
    public static void lifecycle(Path dir) throws Exception {
        Path replacing=dir.resolve("builtin-replaces-existing.parquet");Files.writeString(replacing,"existing-output");
        try(var writer=ParquetFileWriter.create(OutputFile.of(replacing),schema(),config(128))){row(writer,Fixture.rows("semantic").getFirst());Fixture.check(Files.readString(replacing).equals("existing-output"),"existing destination retained until close");}
        try(var f=dev.hardwood.reader.ParquetFileReader.open(InputFile.of(replacing))){Fixture.check(f.getFileMetaData().numRows()==1,"builtin replaces old destination at close");}
        System.out.println(Fixture.json(Map.of("check","builtin_existing_destination","status","PASS","behavior","existing destination replaced atomically; no CREATE_NEW ownership guard","fixed_sibling_suffix",".hardwood-tmp")));
        Path p=dir.resolve("aborted.parquet");
        var w=ParquetFileWriter.create(OutputFile.of(p),schema(),config(128));row(w,Fixture.rows("canonical").getFirst());w.endRowGroup();w.abort();w.abort();w.close();Fixture.check(!Files.exists(p)&&!Files.exists(Path.of(p+".hardwood-tmp")),"abort cleanup");
        System.out.println(Fixture.json(Map.of("check","explicit_abort_idempotent","status","PASS")));
        p=dir.resolve("bad-required.parquet"); w=ParquetFileWriter.create(OutputFile.of(p),schema(),config(128));
        boolean rejected=false;try{w.rowWriter().writeRow(b->b.setString("row_type","OBJECT"));}catch(IllegalArgumentException|IllegalStateException e){rejected=true;}
        Fixture.check(rejected,"missing required must reject");w.close();Fixture.check(!Files.exists(p),"failed write cannot publish");
        System.out.println(Fixture.json(Map.of("check","failed_record_close_discards","status","PASS")));
        p=dir.resolve("external-error.parquet");
        try(var writer=ParquetFileWriter.create(OutputFile.of(p),schema(),config(128))) {row(writer,Fixture.rows("canonical").getFirst());}
        Fixture.check(Files.exists(p),"ordinary close publishes caller-produced prefix; caller must abort on external failure");
        System.out.println(Fixture.json(Map.of("check","caller_failure_requires_abort","status","PASS","observation","close without abort publishes already accepted rows")));
        for(String phase:List.of("write","close")) {
            Path q=dir.resolve("injected-"+phase+".parquet");var sink=new FaultOutput(OutputFile.of(q),phase);var writer=ParquetFileWriter.create(sink,schema(),config(128));row(writer,Fixture.rows("canonical").getFirst());
            boolean failed=false;try{writer.endRowGroup();writer.close();}catch(IOException e){failed=e.getMessage().contains("injected");}
            Fixture.check(failed,"injected IO failure should escape");Fixture.check(!Files.exists(q),"failure must not publish destination");boolean tempBeforeAbort=Files.exists(Path.of(q+".hardwood-tmp"));writer.abort();writer.close();boolean tempAfterAbort=Files.exists(Path.of(q+".hardwood-tmp"));
            if(phase.equals("close")){Fixture.check(tempBeforeAbort&&tempAfterAbort,"violating OutputFile close cleanup contract leaves temp; writer already CLOSED, abort is no-op");sink.discard();}
            Fixture.check(!Files.exists(q)&&!Files.exists(Path.of(q+".hardwood-tmp")),"cleanup after explicit owner discard when required");
            System.out.println(Fixture.json(Map.of("check","injected_"+phase+"_failure","status","PASS","temp_before_abort",tempBeforeAbort,"temp_after_writer_abort",tempAfterAbort,"owner_discard_required",phase.equals("close"),"injector_performed_cleanup",false)));
        }
        p=dir.resolve("closed.parquet");w=ParquetFileWriter.create(OutputFile.of(p),schema(),config(128));row(w,Fixture.rows("canonical").getFirst());w.close();long bytes=Files.size(p);w.close();w.abort();Fixture.check(Files.size(p)==bytes,"closed abort idempotence");
        boolean closedReject=false;try{row(w,Fixture.rows("canonical").getFirst());}catch(IllegalStateException e){closedReject=true;}Fixture.check(closedReject,"write after close must reject");
        System.out.println(Fixture.json(Map.of("check","close_idempotence_and_write_guard","status","PASS")));
        for(String phase:List.of("before-close","after-close","after-force","before-move")) {
            Path target=dir.resolve("crash-"+phase+".parquet");
            Process child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"--enable-native-access=ALL-UNNAMED","-cp",System.getProperty("java.class.path"),HardwoodCore.class.getName(),"crash-child",target.toString(),phase,activeProfile).redirectErrorStream(true).redirectOutput(dir.resolve("crash-"+phase+".log").toFile()).start();
            Fixture.check(child.waitFor(90,java.util.concurrent.TimeUnit.SECONDS),"child timeout");Fixture.check(child.exitValue()==73,"child must halt at selected phase");
            boolean published=Files.exists(target);Fixture.check(published==(phase.equals("after-close")),"crash publication phase="+phase);
            System.out.println(Fixture.json(Map.of("check","subprocess_crash","status","PASS","phase",phase,"published",published,"temp_exists",Files.exists(Path.of(target+".hardwood-tmp")),"exit",73,"power_loss_test",false)));
        }
        ownedLifecycle(dir);
        Path durable=dir.resolve("durable-adapter.parquet"); var sink=new DurableOutput(durable,null);
        try(var writer=ParquetFileWriter.create(sink,schema(),config(128))){row(writer,Fixture.rows("canonical").getFirst());}
        Fixture.check(sink.fileForced&&sink.directoryForced&&Files.exists(durable),"durability adapter actions");
        System.out.println(Fixture.json(Map.of("check","atomic_rename_adapter_separate_experiment","status","PASS","file_force",true,"directory_force",true,"builtin_force","absent by source inspection","power_loss_test",false)));
    }
    static void ownedLifecycle(Path dir)throws Exception {
        Path p=dir.resolve("swath-owned.parquet");var out=new OwnedOutput(p,null);var expected=Fixture.rows("semantic");
        List<String> positions=new ArrayList<>();long beforeCut=0,afterCut=0;
        try(var w=ParquetFileWriter.create(out,schema(),config(128))) {
            w.keyValueMetadata("spike.unrelated","preserved");w.keyValueMetadata("swath.sort.file_final","false");long previous=out.position();
            for(int i=0;i<expected.size();i++){
                row(w,expected.get(i));Fixture.check(out.position()>=previous,"monotone output position");previous=out.position();positions.add(Fixture.json(Map.of("phase","after_row","row",i,"position",out.position(),"file_size",Files.size(p))));
                if(i==500){beforeCut=out.position();positions.add(Fixture.json(Map.of("phase","before_endRowGroup","row",i,"position",beforeCut,"file_size",Files.size(p))));w.endRowGroup();afterCut=out.position();Fixture.check(afterCut>beforeCut&&Files.size(p)==afterCut,"endRowGroup delivers pending bytes to direct channel");positions.add(Fixture.json(Map.of("phase","after_endRowGroup","row",i,"position",afterCut,"file_size",Files.size(p))));out.syncData();}
            }
            w.keyValueMetadata(Fixture.STAMP);
        }
        Files.writeString(dir.resolve("owned-output-positions.jsonl"),String.join("\n",positions)+"\n");
        System.out.println(Fixture.json(Map.of("check","endRowGroup_channel_delivery","status","PASS","before_position",beforeCut,"after_position",afterCut,"positions_recorded",positions.size(),"sync_file_size_equals_position",true,"flush_only_api",false)));
        String md5=HexFormat.of().formatHex(java.security.MessageDigest.getInstance("MD5").digest(Files.readAllBytes(p)));
        Fixture.check(md5.equals(out.md5())&&out.position()==Files.size(p),"stream digest and byte count match independent read");
        Fixture.check(out.dataForces==1&&out.fileForced&&out.directoryForced,"same-channel data force and final durability sequence");
        System.out.println(Fixture.json(Map.of("check","swath_owned_output_adapter","status","PASS","bytes",out.position(),"md5",md5,"data_force_count",out.dataForces,"final_file_force",out.fileForced,"parent_force",out.directoryForced,"rename",false,"power_loss_test",false)));
        Path failed=dir.resolve("owned-force-failure.parquet");var failedOut=new OwnedOutput(failed,null);failedOut.injectDataForceFailure=true;var writer=ParquetFileWriter.create(failedOut,schema(),config(128));row(writer,expected.getFirst());writer.endRowGroup();
        boolean syncFailed=false;try{failedOut.syncData();}catch(IOException e){syncFailed=e.getMessage().contains("injected");}Fixture.check(syncFailed,"injected data force fails");boolean closeFailed=false;try{writer.close();}catch(IOException e){closeFailed=e.getMessage().contains("poison");}writer.close();Fixture.check(closeFailed&&!Files.exists(failed),"poisoned data force cannot finalize or publish");
        System.out.println(Fixture.json(Map.of("check","owned_data_force_failure_poison","status","PASS","close_retry_noop",true,"target_exists",false)));
        Path dirFailed=dir.resolve("owned-directory-force-failure.parquet");var dirOut=new OwnedOutput(dirFailed,null);dirOut.injectDirectoryForceFailure=true;var dirWriter=ParquetFileWriter.create(dirOut,schema(),config(128));row(dirWriter,expected.getFirst());boolean parentFailed=false;try{dirWriter.close();}catch(IOException e){parentFailed=e.getMessage().contains("injected directory");}dirWriter.close();Fixture.check(parentFailed&&dirOut.poisoned&&dirOut.md5()==null&&!Files.exists(dirFailed),"directory force failure cannot expose durable digest or finalized output");
        System.out.println(Fixture.json(Map.of("check","owned_directory_force_failure_poison","status","PASS","file_forced",dirOut.fileForced,"parent_forced",dirOut.directoryForced,"durable_digest_visible",dirOut.md5()!=null,"target_exists",Files.exists(dirFailed))));
        for(String phase:List.of("owned-before-close","owned-after-force","owned-before-parent-force","owned-after-parent-force")) {
            Path target=dir.resolve("crash-"+phase+".parquet");Process child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"--enable-native-access=ALL-UNNAMED","-cp",System.getProperty("java.class.path"),HardwoodCore.class.getName(),"owned-crash-child",target.toString(),phase,activeProfile).redirectErrorStream(true).redirectOutput(dir.resolve("crash-"+phase+".log").toFile()).start();
            Fixture.check(child.waitFor(90,java.util.concurrent.TimeUnit.SECONDS),"child timeout");Fixture.check(child.exitValue()==73,"owned child phase reached");
            boolean readable=false;try(var f=dev.hardwood.reader.ParquetFileReader.open(InputFile.of(target))){readable=true;}catch(IOException|dev.hardwood.reader.ParquetReadException e){Fixture.check(phase.equals("owned-before-close"),"unexpected invalid footer after finish");}
            Fixture.check(readable!=phase.equals("owned-before-close"),"footer readable state");
            System.out.println(Fixture.json(Map.of("check","swath_owned_subprocess_crash","status","PASS","phase",phase,"target_exists",Files.exists(target),"footer_readable",readable,"checkpoint_finalization_simulated",false,"power_loss_test",false)));
        }
    }
    /** Direct CREATE_NEW sink keeps ownership/publication with swath; no rename or hidden sibling. */
    static final class OwnedOutput implements OutputFile {
        final Path target;final String halt;final java.security.MessageDigest digest;FileChannel ch;long position;int dataForces;boolean fileForced,directoryForced,injectDataForceFailure,injectDirectoryForceFailure,poisoned;String finalDigest;
        OwnedOutput(Path target,String halt)throws Exception{this.target=target;this.halt=halt;digest=java.security.MessageDigest.getInstance("MD5");}
        public void create()throws IOException{ch=FileChannel.open(target,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE);}
        public void write(ByteBuffer b)throws IOException{ByteBuffer copy=b.duplicate();int bytes=b.remaining();while(b.hasRemaining())ch.write(b);digest.update(copy);position+=bytes;}
        public long position(){return position;}
        void syncData()throws IOException{try{Fixture.check(Files.size(target)==position&&ch.size()==position,"sync must see all emitted bytes on same channel");if(injectDataForceFailure)throw new IOException("injected data force");ch.force(false);dataForces++;}catch(IOException e){poisoned=true;throw e;}}
        String md5(){return finalDigest;}
        public void close()throws IOException{
            if(ch==null)return;if(poisoned){discard();throw new IOException("poisoned output cannot close successfully");}
            try{
                ch.force(true);fileForced=true;if("owned-after-force".equals(halt))Runtime.getRuntime().halt(73);ch.close();ch=null;
                if("owned-before-parent-force".equals(halt))Runtime.getRuntime().halt(73);
                if(injectDirectoryForceFailure)throw new IOException("injected directory force");
                try(var dir=FileChannel.open(target.getParent(),StandardOpenOption.READ)){dir.force(true);directoryForced=true;}
                if("owned-after-parent-force".equals(halt))Runtime.getRuntime().halt(73);
                finalDigest=HexFormat.of().formatHex(digest.digest());
            }catch(IOException|RuntimeException e){poisoned=true;finalDigest=null;try{discard();}catch(IOException cleanup){e.addSuppressed(cleanup);}throw e;}
        }
        public void discard()throws IOException{if(ch!=null){ch.close();ch=null;}Files.deleteIfExists(target);}
    }
    static final class FaultOutput implements OutputFile {
        final OutputFile d;final String phase;boolean armed;
        FaultOutput(OutputFile d,String phase){this.d=d;this.phase=phase;}
        public void create()throws IOException{d.create();}
        public long position(){return d.position();}
        public void write(ByteBuffer b)throws IOException{if(armed&&phase.equals("write"))throw new IOException("injected write");d.write(b);armed=true;}
        public void close()throws IOException{if(phase.equals("close")){throw new IOException("injected close without cleanup");}d.close();}
        public void discard()throws IOException{d.discard();}
    }
    /** Minimal same-filesystem commit adapter: footer -> force file -> move -> force directory. */
    static final class DurableOutput implements OutputFile {
        final Path target,temp;final String halt;FileChannel ch;long position;boolean fileForced,directoryForced;
        DurableOutput(Path target,String halt){this.target=target;temp=Path.of(target+".hardwood-tmp");this.halt=halt;}
        public void create()throws IOException{ch=FileChannel.open(temp,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE);}
        public void write(ByteBuffer b)throws IOException{while(b.hasRemaining())position+=ch.write(b);}
        public long position(){return position;}
        public void close()throws IOException{if(ch==null)return;ch.force(true);fileForced=true;if("after-force".equals(halt))Runtime.getRuntime().halt(73);ch.close();ch=null;if("before-move".equals(halt))Runtime.getRuntime().halt(73);Files.move(temp,target,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);try(var dir=FileChannel.open(target.getParent(),StandardOpenOption.READ)){dir.force(true);directoryForced=true;}}
        public void discard()throws IOException{if(ch!=null){ch.close();ch=null;}Files.deleteIfExists(temp);}
    }
    public static void main(String[] args)throws Exception {
        for(String forbidden:List.of("org.apache.hadoop.conf.Configuration","org.apache.parquet.hadoop.ParquetFileReader")){try{Class.forName(forbidden);throw new AssertionError("forbidden class resolves: "+forbidden);}catch(ClassNotFoundException expected){}}
        Fixture.check(ClassLoader.getSystemResource("org/apache/hadoop/conf/Configuration.class")==null && ClassLoader.getSystemResource("org/apache/parquet/hadoop/ParquetFileReader.class")==null,"candidate process cannot resolve Apache or Hadoop classes");
        Fixture.check(!System.getProperty("java.class.path").toLowerCase(Locale.ROOT).contains("hadoop"),"candidate classpath must be Hadoop-free");
        String mode=args[0];Path p=Path.of(args[1]);int profileIndex=(mode.equals("geometry")||mode.equals("lifecycle"))?2:3;activeProfile=args.length>profileIndex?args[profileIndex]:"auto";
        Fixture.check(activeProfile.equals("auto")||activeProfile.equals("delta"),"known encoding profile");
        System.out.println(Fixture.json(Map.of("check","encoding_profile","status","PASS","profile",activeProfile,"mode",mode)));
        switch(mode) {
            case "write" -> write(p,args[2]);case "read"->read(p,args[2]);case "geometry"->{geometry(p);byteGeometry(p);timestampConvenienceBoundary(p);}case "lifecycle"->lifecycle(p);
            case "owned-crash-child"->{String phase=args[2];var out=new OwnedOutput(p,phase);var w=ParquetFileWriter.create(out,schema(),config(128));var fixture=Fixture.rows("semantic");for(var r:fixture)row(w,r);w.endRowGroup();if(phase.equals("owned-before-close"))Runtime.getRuntime().halt(73);w.close();throw new AssertionError("phase not reached");}
            case "crash-child"->{String phase=args[2];OutputFile sink=(phase.equals("after-force")||phase.equals("before-move"))?new DurableOutput(p,phase):OutputFile.of(p);var w=ParquetFileWriter.create(sink,schema(),config(128));var fixture=Fixture.rows("canonical");for(int i=0;i<1000;i++)row(w,fixture.get(i));w.endRowGroup();if(phase.equals("before-close"))Runtime.getRuntime().halt(73);w.close();Runtime.getRuntime().halt(73);}
            default->throw new IllegalArgumentException(mode);
        }
    }
}
