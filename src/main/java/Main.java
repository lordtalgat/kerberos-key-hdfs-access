import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        ReadInfoParquet parquet = new ReadInfoParquet();
        FileStatus[] fl = parquet.getListFiles(args[0]);
        for (int i = 0; i < fl.length; i++) {
            System.out.println(fl[i].getPath());
        }
        List<String> lines = parquet.readTextFile(args[0]);
        lines.forEach(l -> System.out.println(l));
    }
}
