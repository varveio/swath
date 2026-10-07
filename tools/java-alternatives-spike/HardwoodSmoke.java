import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.*;
import dev.hardwood.reader.*;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.*;

import java.nio.file.Path;

public class HardwoodSmoke {
    public static void main(String[] args) throws Exception {
        var schema =
                FileSchema.builder("swath_probe")
                        .addColumn(
                                "key",
                                PhysicalType.BYTE_ARRAY,
                                RepetitionType.REQUIRED,
                                new LogicalType.StringType())
                        .addColumn("size", PhysicalType.INT64, RepetitionType.OPTIONAL)
                        .build();
        var config =
                WriterConfig.builder()
                        .codec(CompressionCodec.UNCOMPRESSED)
                        .pageTargetBytes(128)
                        .rowGroupTargetRows(100)
                        .build();
        if (args.length != 1) throw new IllegalArgumentException("Expected output Parquet path");
        var path = Path.of(args[0]);
        try (var w = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
            w.keyValueMetadata("probe", "standalone");
            var rows = w.rowWriter();
            for (int i = 0; i < 513; i++) {
                final int row = i;
                rows.writeRow(
                        r -> r.setString("key", String.format("k%05d", row)).setLong("size", row));
            }
        }
        long count = 0;
        try (var reader = ParquetFileReader.open(InputFile.of(path));
                var rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                if (rows.getLong("size") != count) throw new AssertionError();
                count++;
            }
            System.out.println(
                    "rows="
                            + count
                            + " schema="
                            + reader.getFileSchema()
                            + " metadata="
                            + reader.getFileMetaData().keyValueMetadata());
        }
        if (count != 513) throw new AssertionError();
    }
}
