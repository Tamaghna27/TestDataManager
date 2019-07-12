Language - C# .Net 
Framework - .NETFramework,Version=v4.5
Tool - Visual Studio 2015

There are two projects in the solution provided
#clDM
This is the class library which contains DataManager class, where all the functionalities like laod data,load schema,select,project,
groupBy and show are coded.

loadData()
It takes a path as input and reads the json file mentioed in the path to create a Table based on the data provided
Input - String Path 
Output - Void

loadSchema()
It takes a path as input and reads the json file mentioed in the path to create a dictionary based on the data provided
Input - String Path 
Output - Void

show()
It will display the content of the current table(which contains data from JSON or modified data)
Input - None
Output - Void

project()
It will take array of column names. The final table after projection will contain a new Data Manager with only the specified columns.
Input - String array
Output - new DataManager instance

# TestDM
Test DM is a test project to verify the functionality of DataManager Class Library

