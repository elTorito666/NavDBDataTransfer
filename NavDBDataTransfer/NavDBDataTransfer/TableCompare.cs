using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace NavDBDataTransfer
{
    public static class TableCompare
    {
        public static void CompareTableAndFields()
        {
            IEnumerable<int> mytables = new int[] { 3, 270 };
            //mytables = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 18, 19, 20, 23, 24, 27, 30, 42, 79, 80, 81, 82, 83, 84, 85, 88, 92, 93, 94, 97, 98, 99, 152, 156, 201, 204, 205, 222, 224, 225, 230, 231, 232, 233, 234, 236, 237, 242, 244, 245, 250, 251, 252, 255, 256, 257, 258, 259, 261, 262, 264, 270, 279, 280, 282, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 301, 308, 309, 310, 311, 312, 313, 314, 315, 323, 324, 325, 330, 333, 334, 340, 341, 348, 349, 352, 381, 385, 402, 403, 404, 409, 452, 453, 464, 800, 801, 5050, 5053, 5054, 5055, 5056, 5057, 5058, 5061, 5066, 5067, 5068, 5069, 5070, 5079,5105,5200,5202,5203 ,5206,5208,5210,5212,5213,5217,5218,5220,5401,5404,5600,5603,5611,5700,5715,5717,5718,5719,5720,5721,5722,5723,5769,5813,5814,6502,7002,7004,7012,7014,7301,8618,8619,5001905,5001906,5001907,5001908,5001909,5001943
            //,5041152,5041166,5041168,5041171,5041172,5041193,5041195,5041196,5041202,5041204,5041206,5041207,5041223,5041240,5041241,5041260,5041261,5041270,5041351,5041352,5041365,5041366,5041368,5041369,5041397,5041398,5047670,5047671,5047672,5047673,5047674,5047675};

            using (var db = new NAV_FIELDS_DBEntities())
            {
                // All affected tables 
                var affectedTables = from navfields in db.NavFields09vs18
                                         //where (mytables.Contains(navfields.TableNo)) //Only selected tables
                                     group navfields.TableNo by navfields.TableNo into tablegroup
                                     orderby tablegroup.Key ascending
                                     select tablegroup.Key;

                //Foreach Tableno in Affected Tables
                foreach (var tno in affectedTables)
                {
                    //CompareTableFields.StartCompare(5041171);
                    ListTableDifferences(tno);
                }
            }
        }

        public static void StartCompare(int tableNo)
        {
            using (var db = new NAV_FIELDS_DBEntities())
            {
                // Display all Data from the database 
                var query = from b in db.NavFields09vs18
                            where (b.TableNo == tableNo)
                            orderby b.TableNo, b.FieldNo
                            select b
                            ;
                string TableName = "";
                //System.Diagnostics.Debug.WriteLine("All Columns in the Table:");
                foreach (var item in query)
                {
                    TableName = DataClass.ReplaceChar(item.TableName2018);
                }
                DataTable dt18 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2018, "["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "]");
                DataTable dt09 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2009, "["+Settings.SourceDBName+"].[dbo].[" + Settings.SourceCompany + "$" + TableName + "]");

                foreach (DataRow field in dt18.Rows)
                {
                    foreach (DataColumn property in dt18.Columns)
                    {
                        //System.Diagnostics.Debug.WriteLine("Columnname  is " + property);
                        if (property.ColumnName == "ColumnName")
                        {
                            System.Diagnostics.Debug.WriteLine("ColumName is Columnname  and has Value " + field[property]);
                        }
                        if (property.ColumnName == "ColumnSize")
                        {
                            System.Diagnostics.Debug.WriteLine("ColumSize for " + field[0] + " is " + field[property]);
                            CompareColumnSizeWith2009(dt09, field, property);
                        }
                    }
                }
            }
        }

        static void CompareColumnSizeWith2009(DataTable dt09, DataRow dr18, DataColumn dc18)
        {
            foreach (DataRow field in dt09.Rows)
            {
                foreach (DataColumn property in dt09.Columns)
                {
                    if (field[0].ToString() == dr18[0].ToString())
                    {
                        //Field Exist in 2009 and 2018
                        //System.Diagnostics.Debug.WriteLine("Field # " + field[0] + " ~ "+ dr18[0] +  " # Exist in 2009 and 2018");
                        if (property.ColumnName == "ColumnSize")
                        {
                            int x = 0;
                            Int32.TryParse(field[property].ToString(), out x);
                            int y = 0;
                            Int32.TryParse(dr18[dc18].ToString(), out y);
                            int z = x.CompareTo(y);
                            if (z >= 1)
                            {
                                System.Diagnostics.Debug.WriteLine("Field  " + field[0] + " has Size in 2009: " + field[property] + " vs 2018 Size: " + dr18[dc18]);
                                //System.Diagnostics.Debug.WriteLine("Field  " + property.ColumnName + " exists in 2009 and 2018");
                            }
                        }
                    }
                    //System.Diagnostics.Debug.WriteLine(field[0] + " " + property.ColumnName + " " + field[property].ToString());
                }
            }
        }

        public static void CheckIf2009FieldHasOtherNameAndTypeIn2018(string fieldname09, string tablename)
        {
            DataTable dt18 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2018, "[" + Settings.TargetDBName + "].[dbo].[" + Settings.TargetCompany + "$" + tablename + "]");
            DataTable dt09 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2009, "[" + Settings.SourceDBName + "].[dbo].[" + Settings.SourceCompany + "$" + tablename + "]");


            DataColumnCollection columns = dt18.Columns;
            if (columns.Contains(fieldname09))
            {
                System.Diagnostics.Debug.WriteLine("FieldName in NAV2009: " + fieldname09 + " seems equal as in NAV2018 ");
            }
            else
            {
                System.Diagnostics.Debug.WriteLine("FieldName in NAV2009: " + fieldname09 + " seems NOT EXISTENT in NAV2018");
            }

            
        }


        public static string GetFieldTypeIn2018(string tableName, string fieldName2018)
        {
            DataTable dt18 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2018, "["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + tableName + "]");
            string defaultvalforfield = "";
            foreach (DataRow field in dt18.Rows)
            {
                foreach (DataColumn property in dt18.Columns)
                {
                    //If Field is the field i want to search
                    if (field[0].ToString() == fieldName2018)
                    {
                        //System.Diagnostics.Debug.WriteLine("Found Field " + field[0]);
                        if (property.ColumnName == "DataType")
                        {
                            //System.Diagnostics.Debug.WriteLine("Found Field " + field[0] + " with DataType " + field[property].ToString() + " set default to: "+GetDefaultForDataType(field[property].ToString()));
                            defaultvalforfield = GetDefaultForDataType(field[property].ToString());
                        }
                    }
                }
            }
            return defaultvalforfield;
        }

        public static string GetDefaultForDataType(string strDataType)
        {
            string def = "";
            switch (strDataType)
            {
                case "System.GUID":
                    def = "NEWID() ";
                    break;
                case "System.Media":
                    def = "NEWID()";
                    break;
                case "System.DateTime":
                    def = "SYSDATETIME()";
                    break;
                case "System.Decimal":
                    def = "0";
                    break;
                case "System.String":
                    def = "''";
                    break;
                case "System.Int32":
                    def = "0";
                    break;
                case "System.Byte":
                    def = "0";
                    break;
                case "System.Byte[]":
                    def = "0";
                    break;
                default:
                    def = "";
                    break;
            }
            return def;
        }

        public static void ListTableDifferences(int tableNo)
        {
            using (var db = new NAV_FIELDS_DBEntities())
            {
                // Check for Suspicious Tables
                var susptables = from entryadata in db.NavFields09vs18
                                 where (entryadata.TableNo == tableNo)
                                 orderby entryadata.TableNo
                                 group entryadata by entryadata.TableNo into newGroup
                                 select newGroup;
                foreach (var nameGroup in susptables)
                {
                    string compare = "";
                    //Console.WriteLine($"Key: {nameGroup.Key}");
                    //System.Diagnostics.Debug.WriteLine("--Compare Table No.:" + nameGroup.Key);

                    foreach (var navf in nameGroup)
                    {
                        if (
                            (navf.TableChangedNameIn2018 == 1)
                            || (navf.TableDeletedIn2018 == 1)
                            || (navf.FieldName2009ChangedIn2018 == 1)
                            || (navf.FieldNameLengthChanged == 1)
                            || (navf.FieldTypeChanged == 1)
                            )
                        {
                            compare = "--Compare Table No.:" + tableNo + "(" + navf.TableName2018 + ")";
                        }
                    }
                    System.Diagnostics.Debug.WriteLine(compare);
                }

                // Display all Data from the database 
                var query = from b in db.NavFields09vs18
                            where (b.TableNo == tableNo)
                            orderby b.TableNo, b.FieldNo
                            select b
                            ;
                string TableName = "";

                //Foreach Entry in Table NAV09vs2018
                foreach (var item in query)
                {
                    //System.Diagnostics.Debug.WriteLine("--?????.:" + item.FieldName2018);

                    TableName = DataClass.ReplaceChar(item.TableName2018);

                    if ((item.FieldName2009ChangedIn2018 == 1))
                    {
                        System.Diagnostics.Debug.WriteLine("FieldName Changed: Fieldname NAV2009: " + item.FieldName2009 + " ->  Fieldname NAV2018: " + item.FieldName2018 + " FieldNo[" + item.FieldNo + "]");
                    }
                    if ((item.Length2009 != item.Length2018) & (item.FieldName2009 != null))
                    {
                        //Only if in 2018 is smaller than in 2009
                        if (item.Length2009 > item.Length2018)
                        {
                            System.Diagnostics.Debug.WriteLine("FieldLength Changed: NAV2009: " + item.FieldName2009 + "(" + item.Length2009 + ")   -> in NAV2018: " + item.FieldName2018 + "(" + item.Length2018 + ")" + " FieldNo[" + item.FieldNo + "]");
                        }
                        else
                        {

                        }
                    }
                    if ((item.Type2009 != item.Type2018) & (item.FieldName2009 != null))
                    {
                        System.Diagnostics.Debug.WriteLine("FieldType Changed: NAV2009: " + item.FieldName2009 + "(" + item.Type2009 + ") -> in NAV2018: " + item.FieldName2018 + "(" + item.Type2018 + ")" + " FieldNo[" + item.FieldNo + "]");
                    }


                }
            }
        }
    }
}
