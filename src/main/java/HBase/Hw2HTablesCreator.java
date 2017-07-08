package HBase;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static Utils.CommonConstants.*;


public class Hw2HTablesCreator {
    private static Hw2HTablesCreator instance = new Hw2HTablesCreator();

    private Admin admin;
    private Connection conn;

    private TableName lift_table_name = TableName.valueOf(LIFT_TABLE_NAME);
    private TableName users_table_name = TableName.valueOf(USERS_TABLE_NAME);


    private Hw2HTablesCreator() {
        System.out.println("configure connection");
        // Instantiating configuration object
        Configuration conf = HBaseConfiguration.create();
        // Instantiating Admin object
        try {
            // Instantiating Connection object
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Hw2HTablesCreator getInstance() {
        return instance;
    }

    public void createTables() throws IOException {
        System.out.println("check if tables exist");
        // delete lift and users tables if already exist
        if (admin.tableExists(lift_table_name))
        {
            System.out.println("delete existing lift table");
            admin.disableTable(lift_table_name);
            admin.deleteTable(lift_table_name);
        }
        if (admin.tableExists(users_table_name))
        {
            System.out.println("delete existing users table");
            admin.disableTable(users_table_name);
            admin.deleteTable(users_table_name);
        }
        // create lift and users tables
        System.out.println("calling create lift table");
        createLiftTable();
        System.out.println("calling create users table");
        createUsersTable();
    }

    public Table getTable(TableName table_name) throws IOException {
        return conn.getTable(lift_table_name);
    }

    public void cleanup() throws IOException {
        System.out.println("tables creator cleanup");
        admin.close();
        conn.close();
    }

    private void createLiftTable() throws IOException {

        // read pos/neg lift csv files
        BufferedReader in_pos = new BufferedReader(new InputStreamReader(Hw2HTablesCreator.class.getResourceAsStream(POS_LIFT_PATH)));
        BufferedReader in_neg = new BufferedReader(new InputStreamReader(Hw2HTablesCreator.class.getResourceAsStream(NEG_LIFT_PATH)));
        Iterable<CSVRecord> pos_records = CSVFormat.DEFAULT.parse(in_pos);
        Iterable<CSVRecord> neg_records = CSVFormat.DEFAULT.parse(in_neg);

        System.out.println("creating map pos/neg lift");
        // create a map (x, pos/neg) -> [(y1, pos/neg lift), ..., (yk, pos/neg lift)]
        HashMap<JSONObject,JSONObject> liftMap = new HashMap<>();
        updateLiftMapFromCSV(liftMap, pos_records, POS);
        updateLiftMapFromCSV(liftMap, neg_records, NEG);
        System.out.println("mapped pos/neg lift");

        System.out.println("creating lift table");
        // create the lift table resource with column family - lift
        HTableDescriptor tableDescriptor = new HTableDescriptor(lift_table_name);
        tableDescriptor.addFamily(new HColumnDescriptor(LIFT_TABLE_CF_LIFT));
        admin.createTable(tableDescriptor);
        System.out.println("created lift table");

        Table table = conn.getTable(lift_table_name);

        // add lift data to the table
        List<Put> put_list = new ArrayList<>();

        System.out.println("populating lift table");
        for (Map.Entry<JSONObject, JSONObject> entry : liftMap.entrySet()) {
            // create a Put object with row key - (x, pos/neg)
            Put put = new Put(Bytes.toBytes(entry.getKey().toString()));
            put.addColumn(Bytes.toBytes(LIFT_TABLE_CF_LIFT), Bytes.toBytes(LIFT_TABLE_CF_LIFT_COL_TOP_K), Bytes.toBytes((entry.getValue()).toJSONString()));
            put_list.add(put);
//            table.put(put);
        }
        table.put(put_list);
        System.out.println("populated lift table");

        table.close();
    }

    private void updateLiftMapFromCSV(HashMap<JSONObject,JSONObject> liftMap, Iterable<CSVRecord> records, String posOrNeg)
    {
        for (CSVRecord record : records) {
            String x = record.get(0);
            String y = record.get(1);
            String lift = record.get(2);

            // create a json object representing a key in lift map
            JSONObject key = new JSONObject();
            key.put(x, posOrNeg);

            // create/get the respected lift array (json object)
            JSONObject values;
            if (!liftMap.containsKey(key))
            {
                values = new JSONObject();
                liftMap.put(key, values);
            }
            values = liftMap.get(key);
            // add the new tuple (y, lift) to the respected lift array (json object)
            values.put(y, lift);
        }
    }

    private void createUsersTable() throws IOException {
        System.out.println("creating users table");
        // create the users table resource with column family - window
        HTableDescriptor tableDescriptor = new HTableDescriptor(users_table_name);
        tableDescriptor.addFamily(new HColumnDescriptor(USERS_TABLE_CF_WINDOW));
        admin.createTable(tableDescriptor);
        System.out.println("created users table");
    }

}
