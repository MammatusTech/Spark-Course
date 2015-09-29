package com.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by fadi on 6/14/15.
 */





public class SparkSQLApp {

    private transient SparkConf conf;

    private SparkSQLApp(SparkConf conf) {
        this.conf = conf;
    }


    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
         SQLContext sqlContext = new SQLContext(sc);

        createDataframe(sc, sqlContext);

        querySQLData(sqlContext);

        sc.stop();

    }

    public void createDataframe(JavaSparkContext sc, SQLContext sqlContext ) {
        List<TodoItem> todos = Arrays.asList(
                new TodoItem("George", "Buy a new computer", "Shopping"),
                new TodoItem("John", "Go to the gym", "Sport"),
                new TodoItem("Ron", "Finish the homework", "Education"),
                new TodoItem("Sam", "buy a car", "Shopping"),
                new TodoItem("Janet", "buy groceries", "Shopping"),
                new TodoItem("Andy", "go to the beach", "Fun"),
                new TodoItem("Paul", "Prepare lunch", "Cooking")
        );
        JavaRDD<TodoItem> rdd = sc.parallelize(todos);

        DataFrame dataframe =   sqlContext.createDataFrame(rdd, TodoItem.class);
        sqlContext.registerDataFrameAsTable(dataframe, "todo");

        System.out.println("Total number of TodoItems = [" + rdd.count() + "]\n");

    }


    public void querySQLData(SQLContext sqlContext) {
        
        DataFrame result = sqlContext.sql("SELECT * from todo");

        System.out.println("Show the DataFrame result:\n");
        result.show();

        System.out.println("Select the id column and show its contents:\n");
        result.select("id").show();


    }

    public static void main( String args[] )


    {

        SparkConf conf = new SparkConf();

        conf.setAppName("TODO sparkSQL and cassandra");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");


        SparkSQLApp app = new SparkSQLApp(conf);
        app.run();

    }
}

