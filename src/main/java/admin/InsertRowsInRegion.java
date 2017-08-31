package admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.SecureRandom;

// cc ClusterOperationExample Shows the use of the cluster operations
public class InsertRowsInRegion {


  public static void main(String[] args) throws IOException, InterruptedException {

    Configuration conf = HBaseConfiguration.create();



    // vv ClusterOperationExample
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    String sTable = args[0];
    String sKey = args[1];
    String sRows = args[2];

    System.out.println("Inserting " + sRows + " in table " + sTable + " and key base " + sKey );
    TableName tableName = TableName.valueOf(sTable);
      String easy = RandomString.digits + "ACEFGHJKLMNPQRUVWXYabcdefhijkprstuvwx";
      RandomString tickets = new RandomString(23, new SecureRandom(), easy);


    BufferedMutator mutator = connection.getBufferedMutator(tableName);
    for (int a = 1; a <= Integer.parseInt(sRows); a++) {
          String row = sKey + Character.toString((char) a);

          Put put = new Put(Bytes.toBytes(row));
          put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(tickets.nextString()),
            Bytes.toBytes("val1asdfasddasasdfñasldfbbqbkjasbdfkljasbfkljabsdkljfbaskldfjbaskljdfbalksjbflaksjdbf"));
          System.out.println("Adding row: " + row.toString() );
          mutator.mutate(put);
        }

    mutator.close();

    admin.close();
    connection.close();
  }
}
