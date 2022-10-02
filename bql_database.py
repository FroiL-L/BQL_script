#####################################
# bql_database:
#   Class file for database-related functionality for BQL.
# authors:
#   @Froilan Luna-Lopez
#       University of Nevada, Reno
#####################################

# Libraries
import os
import bql_table as bt

#####################################
# DatabaseHandler:
#   BQL class for database-related functionality
#####################################
class DatabaseHandler():
    tableHandler = bt.TableHandler() # Table-handling object

    #####################################
    # createDatabaseCommand():
    #   Attempts to create a database as a folder within program current working directory.
    # args:
    #   @db_name: Name of database to create.
    # return:
    #   @1: If succeeded to create database.
    #   @0: If failed to create database.
    #####################################
    def createDatabaseCommand(self, db_name):
        if not os.path.isdir(db_name):
            os.mkdir(db_name)
            print("Database " + db_name + " created.")
            return 1
        print("!Failed to create database " + db_name + " because it already exists.")
        return 0

    #####################################
    # dropDatabaseCommand():
    #   Attempts to delete a database within program current working directory.
    # args:
    #   @db_name: Name of database to delete.
    # return:
    #   1: If successfully deleted database.
    #   0: Otherwise.
    #####################################
    def dropDatabaseCommand(self, db_name):
        if os.path.isdir(db_name):
            os.rmdir(db_name)
            print("Database " + db_name + " deleted.")
            return 1
        print("!Failed to delete database " + db_name + " because it does not exist.")
        return 0
