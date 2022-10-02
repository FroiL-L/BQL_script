#####################################
# bql_table:
#   Class file for table-related functionality.
# authors:
#   @Froilan Luna-Lopez
#       University of Nevada, Reno
#####################################

# Libraries
import os
import random

#####################################
# Table:
#   Node to store table contents.
#####################################
class Table:
    # Class variables
    #tpath = ""
    #metadata = []
    #attributes = []
    #content = []

    #####################################
    # Constructor
    #
    # args:
    #   @tname: Name of table to create instance of.
    #####################################
    def __init__(self, *args):
        if len(args) > 0:
            self.tpath = args[0]
            self.lpath = args[0] + "_"
            with open(self.tpath, "r") as df:
                df_lines = df.readlines()
                self.metadata = df_lines[0]
                self.attributes = df_lines[1]
                self.content = [line.replace("\n","") for line in df_lines[2:]]
        else:
            self.tpath = ""
            self.lpath = ""
            self.metadata = []
            self.attributes = []
            self.content = []

    #####################################
    # addAttributes:
    #   Append new attribute and datatype to existing table.
    # args:
    #   @attr_name: Name of attribute to add.
    #   @attr_type: Datatype of attribute to add.
    # return:
    #   None.
    #####################################
    def addAttribute(self, attr_name, attr_type):
        self.metadata = self.metadata.replace("\n", "") + "," + attr_type + "\n"
        self.attributes = self.attributes.replace("\n", "") + "," + attr_name + "\n"

    #####################################
    # saveContent:
    #   Save table content in disk.
    # args:
    #   None.
    # return:
    #   None.
    #####################################
    def saveContent(self):
        with open(self.tpath, "w") as df:
            df.write(self.metadata)
            df.write(self.attributes)
            for line in self.content:
                line = line + "\n"
                df.write(line)

    #####################################
    # tempToOG():
    #   Save contents within intermediate table to original table.
    # args:
    #   NA.
    # return:
    #   NA.
    #####################################
    def tempToOG(self):
        with open(self.tpath, "w") as df:
            with open(self.lpath, "r") as df2:
                for line in df2.readlines():
                    df.write(line)

    #####################################
    # saveTemp():
    #   Save table content in disk in a temporary table.
    # args:
    #   None.
    # return:
    #   None.
    #####################################
    def saveTemp(self):
        with open(self.lpath, "w") as df:
            df.write(self.metadata)
            df.write(self.attributes)
            for line in self.content:
                df.write(line + "\n")

#####################################
# TableHandler:
#   Class with table-related functionality.
#####################################
class TableHandler:
    # Class variables
    valid_datatypes = ["int", "varchar"]
    db_in_use = './'
    lockKey = random.randint(0, 1000)
    
    #####################################
    # useCommand():
    #   Attempts to use a folder as a database within proram current working directory.
    # args:
    #   @db_name: String with name of database to use.
    # return:
    #
    #####################################
    def useCommand(self, db_name):
        if os.path.isdir(db_name):
            self.db_in_use = self.db_in_use + db_name + "/"
            print("Using database " + db_name + ".")
            return 1
        else:
            print("!Failed to use database " + db_name + " because it does not exist.")
            return 0

    #####################################
    # lockTable():
    #   Add lock to a table in current database.
    # args:
    #   tname: String with name of table.
    # return:
    #   NA.
    #####################################
    def lockTable(self, tname):
        # Variables
        databaseLockFName = self.db_in_use + "_locks.tmp"
        writeMode = "a"

        # Test if lock file exists
        if not os.path.isfile(databaseLockFName):
            writeMode = "w"

        # Open file with locks
        with open(databaseLockFName, writeMode) as df:
            df.write(str(self.lockKey) + "," + tname + "\n")

    #####################################
    # isTableLocked():
    #   Tests whether a table is the current database is locked.
    # args:
    #   tname: String with name to table.
    # return:
    #   1: If table is locked.
    #   0: otherwise.
    #####################################
    def isTableLocked(self, tname):
        # Variables
        databaseLockFName = self.db_in_use + "_locks.tmp"

        # Test if lock file exists
        if not os.path.isfile(databaseLockFName):
            return False

        # Open file with locks
        with open(databaseLockFName, "r") as df:
            # Search for table name in lock file
            for line in df.readlines():
                line_split = line.strip().split(",", 1)
                if line_split[1] == tname and self.lockKey != int(line_split[0]):
                    return True
                elif line_split[1] == tname:
                    return None

        return False

    #####################################
    # removeLock():
    #   Removes a lock from a given table.
    # args:
    #   NA.
    # return:
    #   NA.
    #####################################
    def removeLock(self):
        # Variables
        databaseLockFName = self.db_in_use + "_locks.tmp"
        lockFound = False

        # Get lines in current lock file
        with open(databaseLockFName, "r") as df:
            lines = df.readlines()

        # Open file with locks
        with open(databaseLockFName, "w") as df:
            # Search for table name in lock file
            for line in lines:
                line_split = line.split(",", 1)
                if int(line_split[0]) == self.lockKey:
                    lockFound = True
                    continue
                df.write(line)

        return lockFound

    #####################################
    # getLockedFiles():
    #   Get names of tables locked by current user.
    # args:
    #   NA.
    # return:
    #   List with strings of names of tables.
    #####################################
    def getLockedFiles(self):
        # Variables
        databaseLockFName = self.db_in_use + "_locks.tmp"
        lockedFiles = []

        # Open file with locks
        with open(databaseLockFName, "r") as df:
            # Loop through locks
            for lock in df.readlines():
                lock_data = lock.strip().split(",", 1)
                # Test if lock is locked by current user
                if int(lock_data[0]) == self.lockKey:
                    lockedFiles.append(lock_data[1])

        return lockedFiles

    #####################################
    # transationCommand():
    #   Start transaction for current user.
    # args:
    #   NA.
    # return:
    #   NA.
    #####################################
    def transactionCommand(self):
        print("Transaction starts.")

    #####################################
    # commitCommand():
    #   Save all intermediate data into original table.
    # args:
    #   tpath: String with path to original table.
    # return:
    #   NA.
    #####################################
    def commitCommand(self):
        # Get locked files
        lockedFiles = self.getLockedFiles()

        # Save all locked files
        for lockedFile in lockedFiles:
            table = Table(self.db_in_use + "/" + lockedFile + ".bql")
            table.tempToOG()

        # Remove lock from table
        if self.removeLock():
            print("Transaction commited.")
        else:
            print("Transaction abort.")

    #####################################
    # createTableCommand():
    #   Attemps to create a table as a file in current database in use.
    # args:
    #   tokens: Array with tokens passed to create table command.
    # return:
    #   1: If table was successfully created.
    #   0: If table failed to be created.
    #####################################
    def createTableCommand(self, tokens):
        # Test if table already exists
        table_name = tokens[0]
        table_path = self.db_in_use + table_name + ".bql"
        if os.path.isfile(table_path):
            print("!Could not create table because table " + table_name + " already exists.")
            return 0

        # Variables
        table_types = []
        table_names = []
        col_type = []
        syntax_switch = False

        # Organize tokens into a list of attribute names and associated data type
        for token in tokens[1:]:
            if token in self.valid_datatypes:
                col_type.append(token)
                if token != "varchar":
                    syntax_switch = False
                    table_types.append(col_type[0])
                    col_type = []
            else:
                if syntax_switch:
                    col_type.append(token)
                    table_types.append(" ".join(col_type))
                    col_type = []
                    syntax_switch = False
                else:
                    table_names.append(token)
                    syntax_switch = True

        # Create table
        with open(table_path, "w") as df:
            df.write(",".join(table_types))
            df.write("\n")
            df.write(",".join(table_names))
            df.write("\n")

        # Print success
        print("Table " + table_name + " created.")

    #####################################
    # dropTableCommand():
    #   Deletes a table with the current working database.
    # args:
    #   @tname: String with name of table to delete.
    # return:
    #   1: If table successfully deleted.
    #   0: If table failed to delete.
    #####################################
    def dropTableCommand(self, tname):
        # Test if file exists
        table_path = self.db_in_use + tname + ".bql"
        if os.path.isfile(table_path):
            os.remove(table_path)
            print("Table " + tname + " deleted.")
            return 1
        print("!Failed to delete " + tname + " because it does not exist.")
        return 0

    #####################################
    # alterTable():
    #   Alter the structure of a table.
    # args:
    #   tokens: List of tokens with words passed to alter table command.
    # return:
    #   1: If table is successfully altered.
    #   0: If table fails to be altered.
    #####################################
    def alterTableCommand(self, tokens):
        # Variables
        tname = tokens[0]
        alterOp = tokens[1]
        tpath = self.db_in_use + tname + ".bql"
        
        # Test if file exists
        if not os.path.isfile(tpath):
            print("!Failed to alter " + tname + " because it does not exist.")
            return 1

        # Alter table
        add_name = tokens[2]
        add_type = " ".join(tokens[3:])
        table = Table(tpath)
        table.addAttribute(add_name, add_type)

        # Save modified table
        table.saveContent()
        print("Table " + tname + " modified.")

        return 1

    #####################################
    # getAttrPos():
    #   Get position where attribute is positioned.
    # args:
    #   @attrs: List of attributes.
    #   @attrName: Attribute to find.
    # return:
    #   -1: If not found.
    #   Positive otherwise.
    #####################################
    def getAttrPos(self, attrs, attrName):
        # Loop through attributes
        for i in range(0, len(attrs)):
            if attrs[i] == attrName:
                return i

        return -1

    #####################################
    # getAliasStruct():
    #   Build dictionary with alias-name pairs.
    # args:
    #   @aliasTokens: List with strings in form of "name alias".
    # return:
    #   Dictionary with alias as key and name as value.
    #####################################
    def getAliasStruct(self, aliasTokens):
        # Variables
        aliasStruct = {}

        # Loop through aliases
        for alias in aliasTokens:
            alias_split = alias.split(" ")
            if len(alias_split) == 1: # No alias found
                aliasStruct[alias] = alias
            else: # Alias found
                aliasStruct[alias_split[1]] = alias_split[0]

        return aliasStruct

    #####################################
    # getTablesStruct():
    #   Build dictionary with alias-table path pairs.
    # args:
    #   @aliasStruct: Alias-name pairs.
    # return:
    #   Dictionary with alias as key and table path as value.
    #####################################
    def getTablesStruct(self, aliasStruct):
        # Variables
        tablesStruct = {}

        # Loop through aliases
        for apair in aliasStruct.items():
            tablesStruct[apair[0]] = self.db_in_use + apair[1] + ".bql"

        return tablesStruct

    #####################################
    # condOpTest():
    #   Test two values based on valid BQL boolean operations.
    # args:
    #   @operand_1: First operand to test.
    #   @operand_2: Second operand to test.
    #   @condOp: Operator to test operands with.
    # return:
    #   True: If <operand_1 condOp operand_2> is true.
    #   False: Otherwise.
    #####################################
    def condOpTest(self, operand_1, operand_2, condOp):
        if condOp == "=" and str(operand_1) == str(operand_2):
            return True
        elif condOp == "!=" and str(operand_1) != str(operand_2):
            return True
        elif condOp == ">" and float(operand_1) > float(operand_2):
            return True
        elif condOp == "<" and float(operand_1) < float(operand_2):
            return True
        return False

    #####################################
    # innerJoinOp():
    #   Perform inner join on tables.
    # args:
    #   @tables: List with Table objects to join.
    #   @operand_1: Attribute name for first table.
    #   @operand_1: Attribute name for first table or constant.
    #   @condOp: Comparison operation to perform on both operands.
    # return:
    #   New updated Table.
    #####################################
    def innerJoinOp(self, tables, operand_1, operand_2, condOp):
        # Variables
        newTable = Table()
        table_1 = tables[0]

        # Single table mode
        if len(tables) == 1:
            # Update new Table
            newTable.metadata = table_1.metadata
            newTable.attributes = table_1.attributes

            # Variables
            content = table_1.content
            attrPos = self.getAttrPos(table_1.attributes.split(","), operand_1)

            # Loop through all records in table
            for line in content:
                line_split = line.split(",")
                # Add record to line if comparison is true
                if self.condOpTest(line_split[attrPos], operand_2, condOp):
                    newTable.content.append(line)
        # Two table mode
        else:
            table_2 = tables[1]

            # Update new Table
            newTable.metadata = table_1.metadata.strip() + "," + table_2.metadata
            newTable.attributes = table_1.attributes.strip() + "," + table_2.attributes

            # Variables
            content_1 = table_1.content
            content_2 = table_2.content
            attrPos_1 = self.getAttrPos(table_1.attributes.split(","), operand_1)
            attrPos_2 = self.getAttrPos(table_2.attributes.split(","), operand_2)

            # Loop through all records in first table
            for line_1 in content_1:
                line_1_split = line_1.split(",")
                # Loop through all records in second table
                for line_2 in content_2:
                    line_2_split = line_2.split(",")
                    # Ad record to line if comparison is true
                    if self.condOpTest(line_1_split[attrPos_1], line_2_split[attrPos_2], condOp):
                        newTable.content.append(line_1 + "," + line_2)

        return newTable

    #####################################
    # leftOuterJoinOp():
    #   Perform left outer join on two tables with a given test operation.
    # args:
    #   @tables: List with Table objects to join.
    #   @operand_1: Attribute name for first table.
    #   @operand_2: Attribute name for second table.
    #   @condOp: Testing operation to perform on attributes.
    # return:
    #   New updated Table.
    #####################################
    def leftOuterJoinOp(self, tables, operand_1, operand_2, condOp):
        # Variables
        newTable = Table() # New filtered table
        table_1 = tables[0] # First table
        table_2 = tables[1] # Second table
        content_1 = table_1.content # First table records
        content_2 = table_2.content # Second table records
        attrPos_1 = self.getAttrPos(table_1.attributes.split(","), operand_1) # Wanted attribute position in table 1
        attrPos_2 = self.getAttrPos(table_2.attributes.split(","), operand_2) # Wanted attribute position in table 2
        
        # Update new table header values
        newTable.metadata = table_1.metadata.strip() + "," + table_2.metadata
        newTable.attributes = table_1.attributes.strip() + "," + table_2.attributes

        # Loop through all records in first table
        for line_1 in content_1:
            line_1_split = line_1.split(",")
            noMatches = True
            # Loop through all records in second table
            for line_2 in content_2:
                line_2_split = line_2.split(",")
                # If comparison is true, add both
                if self.condOpTest(line_1_split[attrPos_1], line_2_split[attrPos_2], condOp):
                    noMatches = False
                    newTable.content.append(line_1 + "," + line_2)
                # If comparison is false, add first table record and empty spaces
                #else:
                #    newTable.content.append(line_1 + ("," * len(line_2_split)))
            if noMatches:
                newTable.content.append(line_1 + ("," * len(line_2_split)))
        
        return newTable

    #####################################
    # selectCommand()
    #   Selects content wanted from a table.
    # args:
    #   args: Tokens with words passed to select command
    # return:
    #   1: If content was successfully selected.
    #   0: Otherwise.
    #####################################
    def selectCommand(self, commStruct):
        # Variables
        selectTokens = commStruct["select"]
        fromTokens = commStruct["from"]
        whereTokens = commStruct.get("where")
        aliasStruct = self.getAliasStruct(fromTokens)
        tablePaths = self.getTablesStruct(aliasStruct)

        # Test for non-existent table
        for table_path in tablePaths.values():
            if not os.path.isfile(table_path):
                print("!Could not select from " + table_path + " because it does not exist.")
                return 0
        
        # Create table objects
        tables = {}
        for tname in tablePaths:
            tableName = tablePaths[tname]
            print("Locked? " + tname)
            if self.isTableLocked(tname) == None:
                print("Locked")
                tableName += "_"
            tables[tname] = Table(tableName)

        # Perform where condition, if exists
        selectedContent = tables[list(tables.keys())[0]]
        if whereTokens:
            selectedContent = self.onCommand(tables, whereTokens)

        # Perform on condition, if exists
        if "on" in commStruct.keys():
            onTokens = commStruct["on"]
            if "inner join" in commStruct.keys():
                selectedContent = self.onCommand(tables, onTokens)
            elif "left outer join" in commStruct.keys():
                selectedContent = self.onCommand(tables, onTokens, "left outer join")
        tattr = selectedContent.attributes.strip().split(',')

        # Select content
        tmeta = selectedContent.metadata.strip().split(',')
        print()
        # Print all attributes 
        if selectTokens[0] == "*":
            # Print header
            for i in range(len(tmeta)):
                print(tattr[i].replace(" ", "") + " " + tmeta[i], end = "")
                if i != len(tmeta) - 1:
                    print("|", end = "")
            print()
            # Print content
            for line in selectedContent.content:
                line_split = line.split(",")
                print("|".join(line_split))
        # Print selected attributes
        else:
            # Print Header
            for col in colnum:
                print(tattr[col].replace(" ", "") + " " + tmeta[col], end = "|")
            print()
            # Print content
            for line in filteredContent:
                for col in colnum:
                    line_split = line.split(",")
                    print(line_split[col], end = "|")
                print()

        return 1

    #####################################
    # onCommand():
    #   Conditional statements for expclitly stated joins.
    # args:
    #   @tables: Alias-table object pair dictionary.
    #   @condTokens: List of tokens passed to on command.
    #   @joinMode: Type of join to use (default = "inner join")
    # return:
    #   Table object with filtered content.
    #####################################
    def onCommand(self, tables, condTokens, joinMode = "inner join"):
        # Varaibles
        operand_1 = condTokens[0]
        operand_2 = condTokens[2].replace('"', "'")
        testOp = condTokens[1]
        operand_1_access_test = False
        operand_2_access_test = False
        newContent = Table()

        # Test if first operand uses table access method
        if "." in operand_1:
            operand_1_access_test = True

        # Test if second operand uses table access method
        if "." in operand_2:
            operand_2_access_test = True

        # Switch between 
        if not operand_1_access_test and not operand_2_access_test: # No table accessing
            if joinMode == "inner join":
                return self.innerJoinOp([tables[list(tables.keys())[0]]], operand_1, operand_2, testOp)
        elif operand_1_access_test and operand_2_access_test:
            tname_1, aname_1 = operand_1.split(".")
            tname_2, aname_2 = operand_2.split(".")
            if joinMode == "inner join":
                return self.innerJoinOp([tables[tname_1], tables[tname_2]], aname_1, aname_2, testOp)
            elif joinMode == "left outer join":
                return self.leftOuterJoinOp([tables[tname_1], tables[tname_2]], aname_1, aname_2, testOp)

    #####################################
    # insertCommand():
    #   Inserts record into a table.
    # args:
    #   @commandStruct: Command-argument pair dictionary.
    # return:
    #   1: If successfully inserted record into table.
    #   0: Otherwise.
    #####################################
    def insertCommand(self, commandStruct):
        tname = commandStruct.get("insert into")[0]
        tpath = self.db_in_use + tname + ".bql"
        toAdd = commandStruct.get("values")
        # Test if file exists
        if not os.path.isfile(tpath):
            print("!Failed to insert into " + tname + " because it does not exist.")
            return 0

        # Insert record
        table = Table(tpath)
        table.content.append(",".join(toAdd))
        
        # Save modified table
        table.saveContent()
        print("1 new record inserted")

        return 1

    #####################################
    # updateCommand():
    #   Replaces content within an attribute with another string.
    # args:
    #   @tokens: List with words passed to update command
    # return:
    #   1: If successfully performed replacement.
    #   0: Otherwise.
    #####################################
    def updateCommand(self, tokens):
        tname = tokens["update"][0]
        tpath = self.db_in_use + tname + ".bql"

        # Test if file exists
        if not os.path.isfile(tpath):
            print("!Failed: Table " + tname + "does not exist.")
            return 0
        
        # Extrapolate data
        setTokens = tokens["set"]
        condTokens = tokens["where"]
        setAttr = setTokens[0]
        setVal = setTokens[2]
        condAttr = condTokens[0]
        condOp = condTokens[1]
        condVal = condTokens[2]

        # Get to-update attribute column
        table = Table(tpath)
        setColnum = 0
        for attr in table.attributes.replace("\n", "").split(","):
            if attr == setAttr:
                break
            setColnum += 1

        # Get to-test attribute column
        testColnum = 0
        for attr in table.attributes.replace("\n", "").split(","):
            if attr == condAttr:
                break
            testColnum += 1

        # Update content
        newContent = []
        numModified = 0
        for line in table.content:
            line = line.replace("\n", "").split(",")
            if condOp == ">" and float(line[testColnum]) > float(condVal):
                line[setColnum] = setVal
                numModified += 1
            elif condOp == "<" and float(line[testColnum]) < float(condVal):
                line[setColnum] = setVal
                numModified += 1
            elif condOp == "=" and line[testColnum] == condVal:
                line[setColnum] = setVal
                numModified += 1
            elif condOp == "!=" and line[testColnum] != condVal:
                line[setColnum] = setVal
                numModified += 1
            newContent.append(",".join(line))

        # Save modified content
        table.content = newContent
        # Test whether table is already locked by another user.
        if self.isTableLocked(tname):
            print("Error: Table " + tname + " is locked!")
        # Table is not locked by another user.
        else:
            # Lock table for current user.
            self.lockTable(tname)
            # Save modification to intermediate table.
            table.saveTemp()
            # Print modifications
            print(str(numModified) + " records modified.")

        return 1

    #####################################
    # deleteCommand():
    #   Deletes records in table that match condition given.
    # args:
    #   @tokens: List of words passed to delete command
    # return:
    #   1: If successfully deleted records.
    #   0: Otherwise.
    #####################################
    def deleteCommand(self, tokens):
        tname = tokens["from"][0]
        tpath = self.db_in_use + tname + ".bql"
        # Test if file exists
        if not os.path.isfile(tpath):
            print("!Failed to delete from " + tname + " because it does not exist.")
            return 0

        # Variables
        condTokens = tokens["where"]
        condAttr = condTokens[0]
        condOp = condTokens[1]
        condVal = condTokens[2]
        table = Table(tpath)
        newContent = []
        testColnum = 0
        recModified = 0

        # Get attribute column
        for attr in table.attributes.replace("\n", "").split(","):
            if attr == condAttr:
                break
            testColnum += 1

        # Scan through records
        for line in table.content:
            line = line.replace("\n", "").split(",")
            if condOp == ">" and float(line[testColnum]) > float(condVal):
                recModified += 1
                continue
            elif condOp == "<" and float(line[testColnum]) < float(condVal):
                recModified += 1
                continue
            elif condOp == "=" and line[testColnum] == condVal:
                recModified += 1
                continue
            elif condOp == "!=" and line[testColnum] != condVal:
                recModified += 1
                continue
            newContent.append(",".join(line))

        # Test if file is locked

        # Save content
        table.content = newContent
        # Test whether table is already locked by another user.
        if self.isTableLocked(tname):
            print("Error: Table " + tname + " is locked!")
            print("Transaction abort.")
        # Table is not locked by another user.
        else:
            # Lock table for current user.
            self.lockTable(tname)
            # Save modification to intermediate table.
            table.saveTemp()
            # Print modifications
            print(str(numModified) + " records deleted.")

        #table.saveContent()
        #print(str(recModified) + " records deleted.")
        return 1
