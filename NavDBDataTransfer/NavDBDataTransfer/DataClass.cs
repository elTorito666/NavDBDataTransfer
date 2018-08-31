using System;
using System.Data.SqlClient;
using System.Data;

namespace NavDBDataTransfer
{
    public static class DataClass
    {
        public static DataTable GetSQLTableSchema(string _connectionString, string _tableName)
        {
            string connectionString = _connectionString;
            string tableName = _tableName.Trim();
            DataTable schemaTable = new DataTable();
            SqlDataReader sqlDataReader;


            try
            {

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    string queryString = "SELECT * FROM " + tableName;

                    using (SqlCommand queryCMD = new SqlCommand(queryString))
                    {
                        queryCMD.Connection = connection;
                        connection.Open();
                        sqlDataReader = queryCMD.ExecuteReader(CommandBehavior.KeyInfo);
                        schemaTable = sqlDataReader.GetSchemaTable();
                        connection.Close();
                    }

                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            #region Print Table Schema

            foreach (DataRow field in schemaTable.Rows)
            {
                foreach (DataColumn property in schemaTable.Columns)
                {
                    //System.Diagnostics.Debug.WriteLine("{0} - {1}", field[0], property.ColumnName + " = " + field[property].ToString());
                }
            }

            #endregion

            return schemaTable;

        }

        public static string ReplaceChar(string mystring)
        {
            if (mystring.IndexOf("%") != -1)
            {
                //System.Diagnostics.Debug.WriteLine("string contains dog % !");
            }
            mystring = mystring.Replace('%', '_');
            mystring = mystring.Replace('.', '_');
            mystring = mystring.Replace('/', '_');
            return mystring;
        }

        public static string ReplaceCharPoint(string mystring)
        {
            mystring = mystring.Replace('.', '_');
            mystring = mystring.Replace('/', '_');
            return mystring;
        }
    }
}
