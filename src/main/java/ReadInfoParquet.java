import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ReadInfoParquet {
    private Configuration configuration = new Configuration();
    private FileSystem fs = null;

    public ReadInfoParquet() throws  IOException {
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        //set kerberos key access true
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("hadoop.security.authorization", "true");
        configuration.set("hadoop.home.dir", "/");
        configuration.set("fs.defaultFS", "hdfs://localhost:8020");
        UserGroupInformation.setConfiguration(configuration);
        //Using kerberos key to access hadoop server
        UserGroupInformation.loginUserFromKeytab("spark@TEST.LOCAL", "/home/spark/spark.keytab");
        fs = FileSystem.get(configuration);
        System.out.println("Test completed");
    }

    public List<String> readTextFile(String fileName) throws IOException {
        List<String> lines = new ArrayList<>();
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream inputStream = fs.open(new Path(fileName));

        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));) {
            String line = null;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.err);
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
        return lines;
    }

    public FileStatus[] getListFiles(String dir) throws IOException {
        return fs.listStatus(new Path(dir));
    }
}