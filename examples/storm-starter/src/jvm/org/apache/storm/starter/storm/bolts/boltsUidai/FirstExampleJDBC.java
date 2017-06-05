package org.apache.storm.starter.storm.bolts.boltsUidai;

/**
 * Created by anshushukla on 05/01/16.
 */

import java.sql.*;
import java.util.Random;

public class FirstExampleJDBC {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/EMP";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "12345";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            //Insert 1000 row
            //  System.out.println("Inserting records into the table...");
            Random r=new Random();
//            int rnd=r.nextInt(10000);
            stmt = conn.createStatement();
            for(int i=100;i<1000;i++) {
                int rnd=r.nextInt(10000);
                String sqlex = "INSERT INTO Employees  " +
                        "VALUES (" + i + ", 10, 'Ali', 'Fatima')";
//                stmt.executeUpdate(sqlex);
            }

            long startTime = System.nanoTime();
            //STEP 4: Execute a query
//            select * from  Employees where id  BETWEEN 500 and 600 ;
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT id, first, last, age FROM Employees";
//            sql="select * from  Employees where id  BETWEEN 500 and 515";
            ResultSet rs = stmt.executeQuery(sql);

            //STEP 5: Extract data from result set
            while(rs.next()){
                //Retrieve by column name
                int id  = rs.getInt("id");
                int age = rs.getInt("age");
                String first = rs.getString("first");
                String last = rs.getString("last");

                //Display values
                System.out.print("ID: " + id);
                System.out.print(", Age: " + age);
                System.out.print(", First: " + first);
                System.out.println(", Last: " + last);
            }
            //STEP 6: Clean-up environment
            rs.close();
            stmt.close();
            conn.close();

            long stopTime = System.nanoTime();
            System.out.println("in millisec - "+(stopTime - startTime) / (1000000.0));
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }
        finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }//end main
}//end FirstExample
