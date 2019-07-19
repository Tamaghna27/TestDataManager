Language - C# .Net 
Framework - .NETFramework,Version=v4.5
Tool - Visual Studio 2015

To Run the project - 
1) Install Visual Studio 2015 IDE or above from 'https://visualstudio.microsoft.com/' and open the solution(.sln) file.
2) Build Solution and Run/Debug using Menu option.

There are two projects in the solution provided
# clDM
This is the class library which contains DataManager class, where all the functionalities like laod data,load schema,select,project,
groupBy and show are coded.

loadData():
It takes a path as input and reads the json file mentioed in the path to create a Table based on the data provided
Input - String Path 
Output - Void

loadSchema():
It takes a path as input and reads the json file mentioed in the path to create a dictionary based on the data provided
Input - String Path 
Output - Void

show():
It will display the content of the current table(which contains data from JSON or modified data)
Input - None
Output - Void

project():
It will take array of column names. The final table after projection will contain a new Data Manager with only the specified columns.
Input - String array
Output - new DataManager instance

groupBy():
It will take a column name in string format. The final table after group by will contain a new Data Manager with only the specified dimension column and aggregated ( mean ) of measure coluns.
Input - String 
Output - new DataManager instance

select():
It will take a condition as a function and return a new Data Manger instance with only those particular items satisfying the mentioned condition.
Input - function 
Output - new DataManager instance

# TestDM
Test DM is a test project to verify the functionality of DataManager Class Library

Sample Test comands - 
            DataManager dm = new DataManager();
            dm.loadData(@"C:\Users\Arka\Documents\FCTest\cars.json");
            dm.loadSchema(@"C:\Users\Arka\Documents\FCTest\schema.json");
            dm.show();
            DataManager projectDm = dm.project(new string[] { "Maker", "Horsepower", "Origin" });
            projectDm.show();
            DataManager groupByDm = dm.groupBy("Maker"); ;
            groupByDm.show();
            DataManager selectedDm = dm.select(p => (long)p["Displacement"] < 30);
            selectedDm.show();
