
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Web.Script.Serialization;

namespace clDM
{
    public class DataManager
    {
        List<dynamic> dataFeed;
        dynamic[,] arrFeed;        
        Dictionary<string, string> schema;
        dynamic DataIP = new ExpandoObject();

        public void loadData(string sourcePath)
        {
            try
            {
                using (StreamReader r = new StreamReader(sourcePath))
                {
                    string json = r.ReadToEnd();
                    JavaScriptSerializer js = new JavaScriptSerializer();
                    var k = js.Deserialize<dynamic>(json);

                    //Row count
                    int count = 0;
                    foreach (dynamic row in k)
                    {
                        count++;
                    }
                        //Populate First Row
                        int i = 0;
                    foreach (dynamic row in k)
                    {                        
                        int j = 0;
                        if (i == 0)
                        {
                            arrFeed = new dynamic[count, row.Count];
                            foreach (System.Collections.Generic.KeyValuePair<string, dynamic> col in row)
                            {
                                
                                arrFeed[i, j] = (dynamic)col;
                                j++;
                            }
                            i++;
                        }
                        else
                            break;
                    }
                    
                    //dataFeed = JsonConvert.DeserializeObject<List<dynamic>>(json);
                     i = 0;
                    foreach (dynamic row in k)
                    {
                        int j = 0;
                        foreach (System.Collections.Generic.KeyValuePair<string,dynamic> col in row)
                        {
                            
                            arrFeed[i,getIndex(col.Key)] = col;
                                j++;
                        }
                        i++;
                    }

                 
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in load data");
                Console.WriteLine(ex.InnerException);
            }

        }

        private int  getIndex(string colName)
        {            
            int colCount = arrFeed.GetLength(1);
            for (int i=0;i<colCount;i++)
            {
                if (arrFeed[0, i].Key == colName)
                    return i;                
            }
            return 0;

        }

        public void loadSchema(string sourcePath)
        {
            try
            {
                using (StreamReader r = new StreamReader(sourcePath))
                {
                    string json = r.ReadToEnd();
                    JavaScriptSerializer js = new JavaScriptSerializer();
                    var items = js.Deserialize<dynamic>(json);                 
                    schema = new Dictionary<string, string>();

                    var k = ((System.Collections.Generic.Dictionary < string, dynamic>)items).Values;
                    foreach (dynamic item in k)
                    {
                        foreach (System.Collections.Generic.Dictionary<string, dynamic> col in item)
                        {                           
                            schema.Add(col.First().Value, col.Last().Value);
                            //Console.WriteLine(item.name + item.type);
                        }
                          
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

                int rowCount = arrFeed.GetLength(0);
                int colCount = arrFeed.GetLength(1);

                for (int i = 0; i < rowCount; i++)
                {
                    for (int j = 0; j < colCount; j++)
                    {
                        if(i==0)
                        {
                            if (j == colCount - 1)
                                Console.Write(arrFeed[i, j].Key);
                            else
                                Console.Write(arrFeed[i, j].Key + " , ");
                        }
                       
                    }
                    if (i == 0)
                        Console.WriteLine();
                    else
                        break;
                }
                for (int i = 0;i<rowCount;i++)
                {
                    for (int j = 0; j < colCount; j++)
                    {
                        if (j==colCount-1)
                            Console.Write(arrFeed[i,j].Value);
                        else
                            Console.Write(arrFeed[i, j].Value + " , ");
                    }
                    Console.WriteLine();
                }
               
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
                int rowCount = arrFeed.GetLength(0);
                int colCount = arrFeed.GetLength(1);
                dynamic[,] arrFeedProject =  new dynamic[rowCount, schemaNew.Count];
     
                int p = 0;
                for (int j=0;j< colCount;j++)
                {
                    
                    if(arr.ToList().Contains(arrFeed[0,j].Key))
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            arrFeedProject[i, p] = arrFeed[i, j];
                        }
                        p++;
                    }
                }

                dm.arrFeed = arrFeedProject;
                dm.schema = schemaNew;

            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in project operation");
                Console.WriteLine(ex.InnerException);
                dm.arrFeed = arrFeed;
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

                int rowCount = arrFeed.GetLength(0);
                int colCount = arrFeed.GetLength(1);
                dynamic[,] arrFeedProject = new dynamic[rowCount, schemaNew.Count];
                List<dynamic> distinctElem = new List<dynamic>();
              
                int p = 0;
                for (int j = 0; j < colCount; j++)
                {

                    if (schemaNew.Keys.ToArray<string>().ToList().Contains(arrFeed[0, j].Key))
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            arrFeedProject[i, p] = arrFeed[i, j];
                            if(arrFeed[0, j].Key==col)
                            {
                                if (!distinctElem.Contains(arrFeed[i, j].Value))
                                    distinctElem.Add(arrFeed[i, j].Value);
                            }
                        }
                        p++;
                    }
                }
                
                dynamic[,] arrGroupBy= new dynamic[distinctElem.Count, schemaNew.Count];
                colCount = schemaNew.Count;
                int g = 0;
                for (int j = 0; j < colCount; j++)
                {

                    if (arrFeedProject[0, j].Key==col)
                    {
                        
                        for (int i = 0; i < distinctElem.Count; i++)
                        {
                            int repeat = 0;
                            for (int m = 0; m < rowCount; m++)
                            {                                
                               if(distinctElem.ElementAt(i)==arrFeedProject[m,j].Value)
                                {
                                    for(int n=0;n<colCount;n++)
                                    {
                                        if (repeat==0)
                                            arrGroupBy[i, n] = arrFeedProject[m, n];
                                        else if (schemaNew[arrFeedProject[m, n].Key] != "dimension")
                                            arrGroupBy[i, n] = new KeyValuePair<string,dynamic>(arrGroupBy[i, n].Key, (arrGroupBy[i, n].Value==null? 0 : arrGroupBy[i, n].Value) + (arrFeedProject[m, n].Value==null? 0: arrFeedProject[m, n].Value));
                                    }
                                    repeat++;
                                }
                            }
                            for (int n = 0; n < colCount; n++)
                            {
                                if (schemaNew[arrGroupBy[i, n].Key] != "dimension")
                                    arrGroupBy[i, n] = new KeyValuePair<string, dynamic>(arrGroupBy[i, n].Key, Math.Round((decimal)(arrGroupBy[i, n].Value == null ? 0 : arrGroupBy[i, n].Value) / repeat,0));
                            }
                               

                        }
                        g++;
                    }
                }              

                dm.arrFeed = arrGroupBy;
                dm.schema = schemaNew;
                return dm;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Group By operation failed.");
                Console.WriteLine(ex.InnerException);
                dm.arrFeed = arrFeed;
                dm.schema = schema;
                return dm;
            }
        }

        public DataManager select(Func<Dictionary<dynamic,dynamic>, bool> condition)//Func<dynamic, bool> condition
        {
            DataManager dm = new DataManager();

            try
            {
                int rowCount = arrFeed.GetLength(0);
                int colCount = arrFeed.GetLength(1);
                dynamic[,] arrFeedSelect = new dynamic[rowCount, colCount];
                List<dynamic> arr = new List<dynamic>();
                
                int p = 0;
                for (int i = 0; i < rowCount; i++)
                {
                    
                    var t=Enumerable.Range(0, arrFeed.GetLength(1))
                .Select(x => arrFeed[i, x])
                .ToList().ToDictionary(row => row.Key,row => row.Value);
                   
                    if (condition(t))
                    {
                        //copy row
                        for (int j = 0; j < colCount; j++)
                        {
                            arrFeedSelect[p, j] = arrFeed[i, j];
                        }
                        p++;
                    }
                    
                }
                dynamic[,] arrFeedSelect1 = new dynamic[p, colCount];
                for (int i = 0; i < p; i++)
                {
                    for (int j = 0; j < colCount; j++)
                    {
                        arrFeedSelect1[i, j] = arrFeedSelect[i, j];
                    }
                }
                   
                    dm.arrFeed = arrFeedSelect1;
                dm.schema = schema;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Issue in select condition");
                Console.WriteLine(ex.InnerException);
                dm.arrFeed = arrFeed;
                dm.schema = schema;
            }
            return dm;
        }
    }
}
