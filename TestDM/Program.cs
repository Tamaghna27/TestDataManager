using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using clDM;

namespace TestDM
{
    class Program
    {
        static void Main(string[] args)
        {
            DataManager dm = new DataManager();
            dm.loadData(@"C:\Users\Arka\Documents\FCTest\cars.json");
            dm.loadSchema(@"C:\Users\Arka\Documents\FCTest\schema.json");
            dm.show();
            DataManager projectDm = dm.project(new string[] { "Maker", "Horsepower", "Origin" });
            projectDm.show();
            DataManager groupByDm = dm.groupBy("Maker"); 
            groupByDm.show();
            DataManager selectedDm = dm.select(p => (long)p["Displacement"] > 300);
            selectedDm.show();
            Console.ReadKey();
        }
    }
}
