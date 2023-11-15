import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, BytesWritable, Text> {
    private BytesWritable keyBytes = new BytesWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        byte[] line = value.getBytes();
        byte[] keyPart = new byte[10];

        System.arraycopy(line, 0, keyPart, 0, 10);
        keyBytes.set(keyPart, 0, 10);

        context.write(keyBytes, value);
    }
}
