/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Library-independent expected values. Never populated by decoding a candidate file. */
public final class Fixture {
    public static final String[] NAMES = {"key", "size", "last_modified", "etag", "storage_class", "version_id", "is_latest", "is_delete_marker", "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"};
    public static final Map<String,String> STAMP = Map.of("swath.sort.order", "key_bytes_unsigned,version_id_null_first,row_type_rank", "swath.sort.mode", "objects", "swath.sort.format_version", "1", "swath.sort.file_index", "1", "swath.sort.file_final", "true");
    private Fixture() {}
    public static List<Object[]> rows(String kind) {
        ArrayList<Object[]> rows = new ArrayList<>();
        if (kind.equals("delta-adversarial")) {
            long[] extremes={Long.MIN_VALUE,Long.MAX_VALUE,-1L,0L,1L,-1_000_001L,1_700_000_000_123_456L};
            for(int i=0;i<2056;i++){
                String key=String.format(Locale.ROOT,"delta/%08d/",i)+"x".repeat(1009); // 1024 UTF-8 bytes
                Object[] r=row(key,i,i<257?0:i<514?1023:i&1023,"OBJECT");
                if(r[1]!=null)r[1]=extremes[i%extremes.length];if(r[2]!=null)r[2]=extremes[(i+1)%extremes.length];
                if(r[3]!=null)r[3]=switch(i%4){case 0->"";case 1->"shared-etag/"+"z".repeat(1000)+i;case 2->"\u0000é\uffff\ud800\udc00";default->"etag/"+Integer.toHexString(i*7919);};
                if(r[5]!=null)r[5]=switch(i%4){case 0->"";case 1->"v\uffff";case 2->"v\ud800\udc00";default->"v\u0000"+i;};
                rows.add(r);
            }
        } else if(kind.equals("dictionary-transitions")) {
            for(int i=0;i<3072;i++){
                Object[] r=row(String.format(Locale.ROOT,"transition/%08d",i),i,i&1023,"OBJECT");r[1]=(long)i;
                r[9]=i%7==0?null:i/1024==1?"unique-owner-"+i+"-"+Integer.toHexString(i*7919):"repeated-owner";
                rows.add(r);
            }
        } else if (kind.startsWith("boundaries")) {
            String[] edges={"~", "\u007f", "\u0080", "é", "\ud7ff", "\ue000", "\uffff", "\ud800\udc00", "\udbff\udfff"};
            int id=0;for(int block=0;block<4;block++)for(String edge:edges)for(int neighbour=0;neighbour<3;neighbour++)rows.add(row(String.format(Locale.ROOT,"u%02d/",block)+edge+neighbour,id++,1023,"OBJECT"));
            for(String edge:edges)for(int neighbour=0;neighbour<3;neighbour++)rows.add(row("long/"+"x".repeat(1011)+edge+neighbour,id++,1023,"OBJECT"));
        } else if (kind.equals("single") || kind.equals("equal")) {
            for(int i=0;i<(kind.equals("single")?1:384);i++)rows.add(row("same-key",i,1023,"OBJECT"));
        } else if (kind.equals("dense")) {
            for (int i = 0; i < 24576; i++) rows.add(row(String.format(Locale.ROOT,"dense/%08d", i), i, i & 1023, "OBJECT"));
        } else {
            for (int i = 0; i < 1024; i++) rows.add(row(String.format(Locale.ROOT,"nulls/%04d", i), i, i, "OBJECT"));
            String[] keys = {"", "\u0000", "\u0001\t\r\n", "a", "a\u0000", "a/", "é", "Ω", "中", "\ud7ff", "\ue000", "\ufffd", "\uffff", "\ud800\udc00", "\udbff\udfff", "long/" + "x".repeat(1019)};
            for (int i=0;i<keys.length;i++) rows.add(row(keys[i], 2000+i, 1023, "OBJECT"));
            rows.add(row("prefix/", 3000, 0, "COMMON_PREFIX"));
            rows.add(row("deleted", 3001, 1022, "DELETE_MARKER"));
            rows.add(row("same-key", 3002, 0, "OBJECT"));
            rows.add(row("same-key", 3003, 16, "OBJECT"));
            rows.add(row("same-key", 3004, 16, "DELETE_MARKER"));
        }
        if (kind.startsWith("boundaries") || kind.equals("dense") || kind.equals("semantic") || kind.equals("production") || kind.equals("single") || kind.equals("equal")) for (int i=0;i<rows.size();i++) if(rows.get(i)[12].equals("OBJECT")) rows.get(i)[1]=(long)i*100;
        if(kind.equals("production"))for(Object[] r:rows){
            String type=(String)r[12];
            if(r[2]!=null&&(Long)r[2]==0L)r[2]=null; // actual swath timestamp sentinel semantics
            if(type.equals("OBJECT"))r[6]=r[5]==null?null:Boolean.TRUE.equals(r[6]);
            if(type.equals("DELETE_MARKER")){r[6]=Boolean.TRUE.equals(r[6]);for(int c:new int[]{3,4,9,10,11})r[c]=null;}
        }
        rows.sort((a,b) -> {
            int c = Arrays.compareUnsigned(bytes((String)a[0]), bytes((String)b[0]));
            if(c!=0) return c;
            if(a[5] == null && b[5] != null) return -1;
            if(a[5] != null && b[5] == null) return 1;
            if(a[5] != null) { c=Arrays.compareUnsigned(bytes((String)a[5]),bytes((String)b[5])); if(c!=0)return c; }
            return Integer.compare(rank((String)a[12]), rank((String)b[12]));
        });
        return rows;
    }
    private static int rank(String s) {return switch(s) {case "OBJECT" -> 0; case "COMMON_PREFIX" -> 1; default -> 2;};}
    private static Object[] row(String key, int id, int mask, String type) {
        Object[] r = {key, (long)id*100, switch(id%5){case 0 -> -1L; case 1 -> -1_000_001L; case 2 -> 1L; case 3 -> 1_700_000_000_123_456L; default -> 0L;}, "etag-"+id+"-2", "STANDARD", "v\u0000é", (id%2)==0, type.equals("DELETE_MARKER"), "owner\u0000id", "display 中", "CRC32C,SHA256", "FULL_OBJECT", type};
        int[] optional = {1,2,3,4,5,6,8,9,10,11};
        for(int b=0;b<optional.length;b++) if((mask & (1<<b))==0)r[optional[b]]=null;
        if(!type.equals("OBJECT"))r[1]=null;
        if(type.equals("COMMON_PREFIX"))r[2]=null;
        return r;
    }
    public static byte[] bytes(String s) {return s.getBytes(StandardCharsets.UTF_8);}
    public static void check(boolean ok, String message) {if(!ok)throw new AssertionError(message);}
    public static void equal(Object[] actual, Object[] expected, long row) {
        for(int i=0;i<NAMES.length;i++) check(Objects.equals(actual[i],expected[i]), "row="+row+" column="+NAMES[i]+" expected="+expected[i]+" actual="+actual[i]);
    }
    public static String digest(List<Object[]> rows) throws Exception {
        MessageDigest d=MessageDigest.getInstance("SHA-256");
        for(Object[] row: rows) for(Object v:row) {byte[] b = v==null?new byte[0]:bytes(v.toString()); d.update((byte)(v==null?0:1)); d.update(java.nio.ByteBuffer.allocate(4).putInt(b.length).array());d.update(b);}
        return HexFormat.of().formatHex(d.digest());
    }
    public static String json(Map<String,?> map) {
        StringJoiner j=new StringJoiner(",","{","}");map.forEach((k,v)->j.add(quote(k)+":"+(v==null?"null":v instanceof Number || v instanceof Boolean?v:quote(v.toString()))));return j.toString();
    }
    private static String quote(String s) {StringBuilder b=new StringBuilder("\"");for(char c:s.toCharArray()) {switch(c){case '"'->b.append("\\\"");case '\\'->b.append("\\\\");case '\n'->b.append("\\n");case '\r'->b.append("\\r");case '\t'->b.append("\\t");default->{if(c<32)b.append(String.format("\\u%04x",(int)c));else b.append(c);}}}return b.append('"').toString();}
}
