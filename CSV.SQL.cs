/*
Author: Mark Moore
Created 9/5/2017
Read data from a CSV file and write it to a SQL Database.
In this case the SQL Database is local using Windows Entegrated Security
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;


namespace CSV2SQL
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection con = new SqlConnection();
			/* replace Server and Database with your Server and database.
			*/
            con.ConnectionString = "Data Source=Server;Initial Catalog=Databse;Integrated Security=True ;";
            

            string filepath = @"c:\\YourCSV.csv";

            StreamReader sr = new StreamReader(filepath);
            
            string line = sr.ReadLine();
            string[] value = line.Split(',');
            DataTable dt = new DataTable();
            DataRow row;
            Console.Write("Reading CSV File");
            foreach (string dc in value)
            {
                dt.Columns.Add(new DataColumn(dc));
            }

            while (!sr.EndOfStream)
            {
                value = sr.ReadLine().Split(',');
                if (value.Length == dt.Columns.Count)
                {
                    row = dt.NewRow();
                    row.ItemArray = value;
                    dt.Rows.Add(row);
                }
            }

            SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
            bc.DestinationTableName = "hmda_lar";
            bc.BatchSize = dt.Rows.Count;
            bc.BulkCopyTimeout = 21600;
            Console.Write("Writing ", dt.Rows.Count, " Rows to SQL Server");
            con.Open();
            bc.WriteToServer(dt);
            bc.Close();
            con.Close();
        }
    }
}