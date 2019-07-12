using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Reflection;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;

namespace clDM
{
    public class DataManager
    {
        List<dynamic> dataFeed;
        DataTable tblFeed;
        Dictionary<string, string> schema;
        dynamic DataIP = new ExpandoObject();

        public void loadData(string sourcePath)
        {
            try
            {
                using (StreamReader r = new StreamReader(sourcePath))
                {
                    string json = r.ReadToEnd();
                    dataFeed = JsonConvert.DeserializeObject<List<dynamic>>(json);
                    var token = JToken.Parse(json);
                    if (token.Type == JTokenType.Object)
                        token = new JArray(token);
                    tblFeed = token.ToObject<DataTable>();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in load data");
                Console.WriteLine(ex.InnerException);
            }

        }



        public void loadSchema(string sourcePath)
        {
            try
            {
                using (StreamReader r = new StreamReader(sourcePath))
                {
                    string json = r.ReadToEnd();
                    dynamic items = JsonConvert.DeserializeObject(json);
                    schema = new Dictionary<string, string>();
                    DataTable dtFeed = new DataTable();
                    foreach (dynamic item in items.schema)
                    {
                        dtFeed.Columns.Add(item.name.Value);
                        schema.Add(item.name.Value, item.type.Value);
                        //Console.WriteLine(item.name + item.type);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in load schema");
                Console.WriteLine(ex.InnerException);
            }

        }



        public void show()
        {
            try
            {
                foreach (dynamic col in tblFeed.Columns)
                {
                    if (col.Ordinal == tblFeed.Columns.Count - 1)
                        Console.Write(col.ColumnName);
                    else
                        Console.Write(col.ColumnName + " , ");
                }

                foreach (dynamic row in tblFeed.Rows)
                {
                    Console.WriteLine();
                    for (int i = 0; i <= tblFeed.Columns.Count - 1; i++)
                    {
                        if (i == tblFeed.Columns.Count - 1)
                            Console.Write(row[i]);
                        else
                            Console.Write(row[i] + " , ");
                    }
                }
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in show");
                Console.WriteLine(ex.InnerException);
            }
        }



        public DataManager project(string[] arr)

        {
            DataManager dm = new DataManager();
            try
            {
                Dictionary<string, string> schemaNew;
                schemaNew = schema.Where(a => arr.Contains(a.Key)).ToDictionary(entry => entry.Key,
                                                   entry => entry.Value);
                DataView view = new System.Data.DataView(tblFeed);
                DataTable selected = view.ToTable("Selected", false, arr);

                dm.tblFeed = selected;
                dm.schema = schemaNew;

            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in project operation");
                Console.WriteLine(ex.InnerException);
                dm.tblFeed = tblFeed;
                dm.schema = schema;
            }
            return dm;
        }



        public DataManager groupBy(string col)
        {
            DataManager dm = new DataManager();
            try
            {
                Dictionary<string, string> schemaNew;
                schemaNew = schema.Where(a => (a.Key == col && a.Value == "dimension") || (a.Key != col && a.Value != "dimension")).ToDictionary(entry => entry.Key,
                                                   entry => entry.Value);
                DataView view = new System.Data.DataView(tblFeed);
                DataTable selected =
                        view.ToTable("Selected", false, schemaNew.Keys.ToArray<string>());
                foreach (DataRow row in selected.Rows)
                {
                    foreach (var par in schemaNew.Keys)
                    {
                        if (par != col)
                            if (row.IsNull(par))
                                row[par] = 0;
                    }
                }

                DataTable grouped = selected.AsEnumerable().GroupBy(r => r.Field<string>(col)).Select(g =>
                {
                    var row = selected.NewRow();

                    row[col] = g.Key;
                    foreach (var par in schemaNew.Keys)
                    {
                        if (par != col)
                            row[par] = g.Average(r => r.Field<long>(par));

                    }
                    return row;
                }).CopyToDataTable();


                dm.tblFeed = grouped;
                dm.schema = schemaNew;
                return dm;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Group By operation failed.");
                Console.WriteLine(ex.InnerException);
                dm.tblFeed = tblFeed;
                dm.schema = schema;
                return dm;
            }
        }

        public DataManager select(Func<DataRow, bool> condition)//Func<dynamic, bool> condition
        {
            DataManager dm = new DataManager();

            try
            {
                DataTable view = tblFeed.Clone();
                DataTable selected = tblFeed.AsEnumerable().Where(condition).CopyToDataTable();
                dm.tblFeed = selected;
                dm.schema = schema;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in select condition");
                Console.WriteLine(ex.InnerException);
                dm.tblFeed = tblFeed;
                dm.schema = schema;
            }
            return dm;
        }
    }
}
