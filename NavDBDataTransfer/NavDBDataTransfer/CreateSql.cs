using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace NavDBDataTransfer
{
    class CreateSql
    {
        public static void InitMakeTSQL()
        {
            using (var db = new NAV_FIELDS_DBEntities())
            {
                IEnumerable<int> mytables = new int[] { };
                // T-SQL für folgende Tabellen erstellen: 
                //mytables = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 18, 19, 20, 23, 24, 27, 30, 42, 79, 80, 81, 82, 83, 84, 85, 88, 92, 93, 94, 97, 98, 99, 152, 156, 201, 204, 205, 222, 224, 225, 230, 231, 232, 233, 234, 236, 237, 242, 244, 245, 250, 251, 252, 255, 256, 257, 258, 259, 261, 262, 264, 270, 279, 280, 282, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 301, 308, 309, 310, 311, 312, 313, 314, 315, 323, 324, 325, 330, 333, 334, 340, 341, 348, 349, 352, 381, 385, 402, 403, 404, 409, 452, 453, 464, 800, 801, 5050, 5053, 5054, 5055, 5056, 5057, 5058, 5061, 5066, 5067, 5068, 5069, 5070, 5079,5105,5200,5202,5203 ,5206,5208,5210,5212,5213,5217,5218,5220,5401,5404,5600,5603,5611,5700,5715,5717,5718,5719,5720,5721,5722,5723,5769,5813,5814,6502,7002,7004,7012,7014,7301,8618,8619,5001905,5001906,5001907,5001908,5001909,5001943
                //,5041152,5041166,5041168,5041171,5041172,5041193,5041195,5041196,5041202,5041204,5041206,5041207,5041223,5041240,5041241,5041260,5041261,5041270,5041351,5041352,5041365,5041366,5041368,5041369,5041397,5041398,5047670,5047671,5047672,5047673,5047674,5047675};

                //oder nur eine Tabelle
                mytables = new int[] { 50 };

                // affected tables  in DB 
                var affectedTables = from navfields in db.NavFields09vs18
                                     where (mytables.Contains(navfields.TableNo))
                                     group navfields.TableNo by navfields.TableNo into tablegroup
                                     orderby tablegroup.Key ascending
                                     select tablegroup.Key;

                foreach (var tno in affectedTables)
                {
                    System.Diagnostics.Debug.WriteLine("--Create TSQL for TableNo " + tno);
                    RunCreateCreateTSqlS(tno);
                }
            }
        }

        static void RunCreateCreateTSqlS(int tableNo)
        {

            using (var db = new NAV_FIELDS_DBEntities())
            {
                // Display all Data from the database 
                var query = from b in db.NavFields09vs18
                            where (b.TableNo == tableNo && b.Class2018 != "FlowField" && b.Class2018 != "FlowFilter" && b.Enabled2018 == "Ja")
                            orderby b.TableNo, b.FieldNo
                            select b
                            ;
                string Columns2018 = "";
                string TableName = "";
                //System.Diagnostics.Debug.WriteLine("All Columns in the Table:");
                foreach (var item in query)
                {
                    Columns2018 = Columns2018 + '[' + DataClass.ReplaceChar(item.FieldName2018) + ']' + ',';
                    TableName = DataClass.ReplaceChar(item.TableName2018);
                }
                Columns2018 = Columns2018.Remove(Columns2018.Length - 1);
                string sqlcmd = "";
                string sqlcmddelete = "";
                if (Settings.WithDeleteStatements == true)
                {
                    sqlcmddelete = sqlcmddelete + "DELETE FROM ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "]";
                }
                sqlcmd = sqlcmd + "INSERT INTO ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "] (";

                sqlcmd = sqlcmd + Columns2018 + ") ";

                DataTable dt18 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2018, "["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "]");
                DataTable dt09 = DataClass.GetSQLTableSchema(Settings.ConnStringNav2009, "["+Settings.SourceDBName+"].[dbo].[" + Settings.SourceCompany + "$" + TableName + "]");



                sqlcmd = sqlcmd + " SELECT ";
                if (Settings.SelectNRowsToInsert > 0)
                {
                    sqlcmd = sqlcmd + " TOP (" + Settings.SelectNRowsToInsert.ToString() + ") ";
                }
                string Columns2009 = "";
                foreach (var item in query)
                {
                    if (item.FieldName2009 != null)
                    {
                        if (item.FieldName2009ChangedIn2018 == 1)
                        {
                            //System.Diagnostics.Debug.WriteLine("FNo: "+ item.FieldNo +" Field2009: " + item.FieldName2009 + " F18: " + item.FieldName2018 + " Type" + item.Type2018);
                            Columns2009 = Columns2009 + TableCompare.GetFieldTypeIn2018(TableName, DataClass.ReplaceCharPoint(item.FieldName2018)) + ",";
                        }
                        else if (CheckTypeIn2009Equivalent(item.FieldName2009, dt09) != "")
                        {
                            Columns2009 = Columns2009 + CheckTypeIn2009Equivalent(item.FieldName2009, dt09) + ',';
                        }
                        else
                        {
                            Columns2009 = Columns2009 + '[' + DataClass.ReplaceCharPoint(item.FieldName2009) + ']' + ',';
                        }
                    }
                    else
                    {
                        //System.Diagnostics.Debug.WriteLine("FNo: " + item.FieldNo + " Field2009: " + item.FieldName2009 + " F18: " + item.FieldName2018 + " Type" + item.Type2018);
                        // TODO: Check which Default value must put here if Field exist in 2018 and not in 2009, and Field are not NULL Allowed
                        switch (item.Type2018)
                        {
                            case "GUID":
                                Columns2009 = Columns2009 + "NEWID() ,";
                                break;
                            case "Media":
                                Columns2009 = Columns2009 + "NEWID() ,";
                                break;
                            case "DateTime":
                                Columns2009 = Columns2009 + "SYSDATETIME() ,";
                                break;
                            case "Decimal":
                                Columns2009 = Columns2009 + "0 ,";
                                break;
                            case "RecordID":
                                Columns2009 = Columns2009 + "cast('' as varbinary(50)) ,";
                                break;
                            default:
                                Columns2009 = Columns2009 + " '',";
                                break;
                        }
                    }
                }
                if (Columns2009.EndsWith(","))
                    Columns2009 = Columns2009.Remove(Columns2009.Length - 1);

                sqlcmd = sqlcmd + Columns2009 + " ";

                sqlcmd = sqlcmd + " FROM [SQLTEST].["+Settings.SourceDBName+"].[dbo].[" + Settings.SourceCompany + "$" + TableName + "] t1 ";

                string strKeyFields = "";
                string strIdentityON = "";
                string strIdentityOFF = "";
                foreach (DataRow field in dt18.Rows)
                {
                    foreach (DataColumn property in dt18.Columns)
                    {
                        if (property.ColumnName == "IsKey")
                        {
                            if (field[property].ToString() == "True")
                            {
                                strKeyFields = strKeyFields + "[" + field[0] + "],";
                                //System.Diagnostics.Debug.WriteLine(field[0] + " " + property.ColumnName + " " + field[property].ToString());
                            }
                        }
                        if (property.ColumnName == "IsIdentity")
                        {
                            //System.Diagnostics.Debug.WriteLine(field[0] + " " + property.ColumnName + " " + field[property].ToString());
                            if (field[property].ToString() == "True")
                            {
                                strIdentityON = "SET IDENTITY_INSERT ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "] ON";
                                strIdentityOFF = "SET IDENTITY_INSERT ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "] OFF";
                            }
                        }
                    }
                }

                if (strKeyFields.Count() > 1)
                    strKeyFields = strKeyFields.Remove(strKeyFields.Length - 1);
                string checkKeys = "";
                string[] s = strKeyFields.Split(',');
                foreach (string w in s)
                {
                    checkKeys = checkKeys + "t2." + w + "=t1." + w + " " + CheckFieldForCollate(w, TableName, dt18) + " AND ";
                }
                checkKeys = checkKeys.Remove(checkKeys.Length - 4);

                //sqlcmd = sqlcmd + " WHERE NOT " + strKeyFields + " COLLATE Latin1_General_100_CS_AS IN (SELECT " + strKeyFields + " FROM ["+Settings.TargetDBName+"].[dbo].["+ TargetCompany + "$" + TableName + "]) ";
                sqlcmd = sqlcmd + " WHERE NOT EXISTS(SELECT " + strKeyFields + " FROM ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$" + TableName + "] t2 WHERE " + checkKeys + ")";

                foreach (DataRow field in dt09.Rows)
                {
                    //System.Diagnostics.Debug.WriteLine(field[0]);
                    //sqlcmd = sqlcmd + "["+ DataClass.ReplaceChar(field[0].ToString())+"],";
                }


                if (TableName == "Payment Terms") { }
                //System.Diagnostics.Debug.WriteLine("DELETE FROM["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$Payment Terms]  WHERE[Last Modified Date Time] >= Convert(datetime, '2018-04-01')");

                if (TableName == "Country_Region")
                {
                    //System.Diagnostics.Debug.WriteLine("DELETE FROM ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$Country_Region] WHERE Id <> CAST('00000000-0000-0000-0000-000000000000' AS UNIQUEIDENTIFIER)");
                    //sqlcmd = sqlcmd.Replace("[" + Settings.SourceCompany + "$Country_Region]", "[Country_Region]");
                }

                //if (TableName == "Shipment Method")
                //System.Diagnostics.Debug.WriteLine("DELETE FROM ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$Shipment Method] WHERE Id <> CAST('00000000-0000-0000-0000-000000000000' AS UNIQUEIDENTIFIER)");

                //if (TableName == "Customer")
                //System.Diagnostics.Debug.WriteLine("DELETE FROM ["+Settings.TargetDBName+"].[dbo].[" + Settings.TargetCompany + "$Customer] WHERE Image <> CAST('00000000-0000-0000-0000-000000000000' AS UNIQUEIDENTIFIER)");

                System.Diagnostics.Debug.WriteLine(sqlcmddelete);
                if (strIdentityOFF != "")
                {
                    System.Diagnostics.Debug.WriteLine(strIdentityOFF);
                }
                System.Diagnostics.Debug.WriteLine(sqlcmd);
                if (strIdentityON != "")
                {
                    System.Diagnostics.Debug.WriteLine(strIdentityON);
                }


                //System.Diagnostics.Debug.WriteLine("--=================================");
            }

            string CheckTypeIn2009Equivalent(string fieldname, DataTable dt09)
            {
                string s = "";

                foreach (DataRow field in dt09.Rows)
                {
                    foreach (DataColumn property in dt09.Columns)
                    {
                        if (property.ColumnName == "IsKey")
                        {
                        }
                        if (fieldname == field[0].ToString())
                        {
                            if (property.ColumnName == "DataType")
                            {
                                //System.Diagnostics.Debug.WriteLine("Fieldname in CheckTypeIn2009Equivalent Check is " + field[0]);
                                //.Diagnostics.Debug.WriteLine(field[0] + " " + property.ColumnName + " " + field[property].ToString());
                                if (field[property].ToString() == "System.Byte[]")
                                {
                                    if (field[0].ToString() == "Picture")
                                        s = "0x";
                                    else
                                        s = "NEWID() ";
                                }
                            }

                        }
                    }
                }
                return s;
            }

            string CheckFieldForCollate(string fieldname, string tablename, DataTable dt18)
            {
                fieldname = fieldname.Replace("[", "");
                fieldname = fieldname.Replace("]", "");
                string colstr = "";
                foreach (DataRow field in dt18.Rows)
                {
                    foreach (DataColumn property in dt18.Columns)
                    {
                        if (property.ColumnName == "IsKey")
                        {
                        }
                        if (fieldname == field[0].ToString())
                        {
                            if (property.ColumnName == "DataType")
                            {
                                if (field[property].ToString() == "System.String")
                                {
                                    colstr = "COLLATE Latin1_General_100_CS_AS ";
                                }
                            }

                        }
                    }
                }
                return colstr;
            }


        }
    }
}
