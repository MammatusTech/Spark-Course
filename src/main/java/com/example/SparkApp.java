package com.example;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

//import org.apache.cassandra.cql.BatchStatement;


/**
 * Created by fadi on 5/18/15.
 */
public class SparkApp implements Serializable {


    static final Logger logger = LoggerFactory.getLogger(SparkApp.class);

    TodoItem item = new TodoItem("George", "Buy a new computer", "Shopping");
    TodoItem item2 = new TodoItem("John", "Go to the gym", "Sport");
    TodoItem item3 = new TodoItem("Ron", "Finish the homework", "Education");
    TodoItem item4 = new TodoItem("Sam", "buy a car", "Shopping");
    TodoItem item5 = new TodoItem("Janet", "buy groceries", "Shopping");
    TodoItem item6 = new TodoItem("Andy", "go to the beach", "Fun");
    TodoItem item7 = new TodoItem("Paul", "Prepare lunch", "Coking");



    String keyspace = "CREATE KEYSPACE IF NOT EXISTS todolist  WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}";

    //index data
    String task1 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item.toString();

    String task2 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item2.toString();

    String task3 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item3.toString();

    String task4 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item4.toString();

    String task5 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item5.toString();

    String task6 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item6.toString();

    String task7 = "INSERT INTO todolisttable (ID, Description, Category, Date)"

            + item7.toString();


    //delete keyspace
    String deletekeyspace = "DROP KEYSPACE IF EXISTS todolist";

    //delete table
    String deletetable = "DROP TABLE todolisttable";

    //create table
    String table = "CREATE TABLE todolist.todolisttable(id text PRIMARY KEY, "
            + "description text, "
            + "category text, "
            + "date timestamp )";

    String tableRDD = "CREATE TABLE todolist.temp(id text PRIMARY KEY, "
            + "description text, "
            + "category text )";

    //Query all data
    String query = "SELECT * FROM todolist.todolisttable";

    String query1 = "SELECT * FROM todolist.temp";
    //Update table
    String update = "UPDATE todolisttable SET Category='Fun',Description='Go to the beach' WHERE ID='Ron'";

    //Deleting data where the index id = George
    String delete = "DELETE FROM todolisttable WHERE ID='George'";

    //Deleting all data
    String deleteall = "TRUNCATE todolisttable";

//---------------------------------------------------------------------------------


    private transient SparkConf conf;

    private SparkApp(SparkConf conf) {
        this.conf = conf;
    }

    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        createSchema(sc);
        loadData(sc);
        saveRDDToCassandra(sc);
        queryData(sc);
        accessTableWitRDD(sc);

        sc.stop();

    }



    private void createSchema(JavaSparkContext sc) {

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        try (Session session = connector.openSession()) {

            session.execute(deletekeyspace);
            session.execute(keyspace);
            session.execute("USE todolist");
            session.execute(table);
            session.execute(tableRDD);


        }
    }

    private void loadData(JavaSparkContext sc) {

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        try (Session session = connector.openSession()) {
            session.execute(task1);
            session.execute(task2);
            session.execute(task3);
            session.execute(task4);
            session.execute(task5);
            session.execute(task6);
            session.execute(task7);

        }



    }
    private void queryData(JavaSparkContext sc) {

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        try (Session session = connector.openSession()) {

            ResultSet results = session.execute(query);

            System.out.println("\nQuery all results from cassandra's todolisttable:\n" + results.all());

            ResultSet results1 = session.execute(query1);

            System.out.println("\nSaving RDD into a temp table in casssandra then query all results from cassandra:\n" + results1.all());


        }

    }

    public  void accessTableWitRDD(JavaSparkContext sc){

        JavaRDD<String> cassandraRDD = javaFunctions(sc).cassandraTable("todolist", "todolisttable")
                .map(new Function<CassandraRow, String>() {
                    @Override
                    public String call(CassandraRow cassandraRow) throws Exception {
                        return cassandraRow.toString();
                    }
                });
        System.out.println("\nReading Data from todolisttable in Cassandra with a RDD: \n" + StringUtils.join(cassandraRDD.toArray(), "\n"));


        // javaFunctions(cassandraRDD).writerBuilder("todolist", "todolisttable", mapToRow(String.class)).saveToCassandra();
    }


    public void saveRDDToCassandra(JavaSparkContext sc) {
        List<TodoItem> todos = Arrays.asList(
                new TodoItem("George", "Buy a new computer", "Shopping"),
                new TodoItem("John", "Go to the gym", "Sport"),
                new TodoItem("Ron", "Finish the homework", "Education"),
                new TodoItem("Sam", "buy a car", "Shopping"),
                new TodoItem("Janet", "buy groceries", "Shopping"),
                new TodoItem("Andy", "go to the beach", "Fun"),
                new TodoItem("Paul", "Prepare lunch", "Coking")
        );
        JavaRDD<TodoItem> rdd = sc.parallelize(todos);
        javaFunctions(rdd).writerBuilder("todolist", "temp", mapToRow(TodoItem.class)).saveToCassandra();



    }



//----------------------------------------------------------------------------------------------------------------------------

    public static void main( String args[] )


    {

        SparkConf conf = new SparkConf();

        conf.setAppName("TODO spark and cassandra");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");


        SparkApp app = new SparkApp(conf);
        app.run();

    }
}