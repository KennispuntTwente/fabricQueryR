import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** Compile without Spark jars; exercise Spark and Hadoop through their public APIs. */
public final class FabricQueryRBatchProbe {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Expected output URI and marker");
        Class<?> sessionType = Class.forName("org.apache.spark.sql.SparkSession");
        Object builder = sessionType.getMethod("builder").invoke(null);
        Object spark = builder.getClass().getMethod("getOrCreate").invoke(builder);
        Object rows = sessionType.getMethod("range", long.class).invoke(spark, 3L);
        long count = (Long) Class.forName("org.apache.spark.sql.Dataset")
            .getMethod("count").invoke(rows);
        if (count != 3L) throw new IllegalStateException("Unexpected Spark count");
        Object context = sessionType.getMethod("sparkContext").invoke(spark);
        Object configuration = Class.forName("org.apache.spark.SparkContext")
            .getMethod("hadoopConfiguration").invoke(context);
        Class<?> pathType = Class.forName("org.apache.hadoop.fs.Path");
        Object path = pathType.getConstructor(String.class).newInstance(args[0]);
        Object fs = pathType.getMethod("getFileSystem", Class.forName("org.apache.hadoop.conf.Configuration"))
            .invoke(path, configuration);
        try (OutputStream output = (OutputStream) Class.forName("org.apache.hadoop.fs.FileSystem")
                .getMethod("create", pathType, boolean.class).invoke(fs, path, false)) {
            output.write((args[1] + ":" + count).getBytes(StandardCharsets.UTF_8));
        }
        sessionType.getMethod("stop").invoke(spark);
    }
}
