package dsparq.load;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class AdjListOutputFormat<K, V> extends FileOutputFormat<K, V> {

  protected static class AdjListRecordWriter<K, V> implements RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";

    private DataOutputStream out;

    public AdjListRecordWriter(DataOutputStream out) throws IOException {
      this.out = out;
      out.writeBytes("<results>\n");
    }

    /**
     * Write the object to the byte stream, handling Text as a special case.
     *
     * @param o
     *          the object to print
     * @throws IOException
     *           if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    private void writeKey(Object o, boolean closing) throws IOException {
      out.writeBytes("<");
      if (closing) {
        out.writeBytes("/");
      }
      writeObject(o);
      out.writeBytes(">");
      if (closing) {
        out.writeBytes("\n");
      }
    }

    public synchronized void write(K key, V value) throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;

      if (nullKey && nullValue) {
        return;
      }

      Object keyObj = key;

      if (nullKey) {
        keyObj = "value";
      }

      writeKey(keyObj, false);

      if (!nullValue) {
        writeObject(value);
      }

      writeKey(keyObj, true);
    }

    public synchronized void close(Reporter reporter) throws IOException {
      try {
        out.writeBytes("</results>\n");
      } finally {
        // even if writeBytes() fails, make sure we close the stream
        out.close();
      }
    }
  }

  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, progress);
    return new AdjListRecordWriter<K, V>(fileOut);
  }
}
