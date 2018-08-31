using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Data;


namespace NavDBDataTransfer
{


    class Program
    {
        

        static void Main(string[] args)
        {
            System.Diagnostics.Debug.WriteLine("Start Main ... ");
            System.Diagnostics.Debug.WriteLine("Test Second Commit to GitHub ... ");
            System.Diagnostics.Debug.WriteLine("Test  Commit to GitHub 31.08.18... ");

            //Make SQL Scripts

            CreateSql.InitMakeTSQL();
            //Compare Tables , Fields ... 
            TableCompare.CompareTableAndFields();
        }

    }
}
