#####################################
# bql_main:
#   Runs command-line querying interpreter for BQL.
# authors:
#   @Froilan Luna-Lopez
#       University of Nevada, Reno
#####################################

# Libraries
import bql_database as bdb
import bql_table as bt

#####################################
# BQLBase:
#   Interpreter class for BQL commands.
#####################################
class BQLBase:
    valid_keywords = ["create",
            "drop",
            "alter",
            "table",
            "database",
            "select",
            "use",
            "insert",
            "into",
            "values",
            "update",
            "set",
            "where",
            "delete",
            "from",
            "on",
            "left",
            "outer",
            "join",
            "inner",
            "commit",
            "begin",
            "transaction"] # BQL keywords
    decorator_keywords = ["left",
            "outer",
            "join",
            "inner"]

    #####################################
    # runCommand():
    #   Runs user BQL commands.
    # args:
    #   @database: BQL DatabaseHandler object to store database info.
    #   @tokens: List with words passed as input to program.
    # return:
    #   None.
    #####################################
    def runCommand(self, database, tokens):
        # Variables
        commandStruct = self.buildCommandStruct(tokens)
        firstCommand = list(commandStruct.keys())[0]
        argTokens = commandStruct[firstCommand]
        #argsIndex = 0
        #command = ""


        # Separate command from query
        #tokens_len = len(tokens)
        #for i in range(0, tokens_len, 1):
        #    if tokens[i] in self.valid_keywords:
        #        argsIndex += 1
        #    else:
        #        break
        #command = " ".join(tokens[:argsIndex])

        # Run appropriate command
        #argTokens = tokens[argsIndex:]
        if firstCommand == "create table":
            database.tableHandler.createTableCommand(argTokens)
        elif firstCommand == "drop table":
            database.tableHandler.dropTableCommand(argTokens[0])
        elif firstCommand == "create database":
            database.createDatabaseCommand(argTokens[0])
        elif firstCommand == "drop database":
            database.dropTableCommand(argTokens[0])
        elif firstCommand == "select":
            database.tableHandler.selectCommand(commandStruct)
        elif firstCommand == "use":
            database.tableHandler.useCommand(argTokens[0])
        elif firstCommand == "alter table":
            database.tableHandler.alterTableCommand(argTokens)
        elif firstCommand == "insert into":
            database.tableHandler.insertCommand(commandStruct)
        elif firstCommand == "update":
            database.tableHandler.updateCommand(commandStruct)
        elif firstCommand == "delete from":
            database.tableHandler.deleteCommand(commandStruct)
        elif firstCommand == "commit":
            database.tableHandler.commitCommand()
        elif firstCommand == "begin transaction":
            database.tableHandler.transactionCommand()
        else:
            print("!Failed to run query because '" + firstCommand + "' is not a valid command.")
            return 0
        return 1

    #####################################
    # buildCommandStruct():
    #   Create command structure for BQL processing.
    # args:
    #   @tokens: List with tokens for BQL command.
    # return:
    #   List with list clause structures.
    #####################################
    def buildCommandStruct(self, tokens):
        # Variables
        commStruct = {}
        command = ""
        commandArgs = []

        # Loop through tokens
        for token in tokens:
            # Key word found
            if token in self.valid_keywords:
                if commandArgs and "join" not in command: # Add new command if not join
                    commStruct[command[:-1]] = commandArgs
                    command = ""
                    commandArgs = []
                elif commandArgs:
                    commStruct[command[:-1]] = ""
                    commStruct[list(commStruct.keys())[-2]] += commandArgs
                    command = ""
                    commandArgs = []
                command += token + " " # Build command
            # Argument found
            else:
                commandArgs.append(token)

        # Add trailing command
        if command:
            commStruct[command[:-1]] = commandArgs

        return commStruct

    #####################################
    # tokenize():
    #   Converts a command string into a list of arguments.
    # args:
    #   @command: String with words to tokenize.
    # return:
    #   None.
    #####################################
    def tokenize(self, command):
        # Variables
        delimiters = [' ', ',', '(', ')', ';', '\t']
        alias_delimit = [',', ';']
        tokens = []
        token_in_progress = ''
        alias_in_progress = []
        allowVars = False

        # Build tokens and token list
        for char in command:
            if char in delimiters: # Delimiter found
                if token_in_progress != '': # Empty token edge case
                    if allowVars:   # Alias delimitting mode
                        if char in alias_delimit:   # Alias delimitter found
                            alias_in_progress.append(token_in_progress)
                            tokens.append(" ".join(alias_in_progress))
                            token_in_progress = ''
                            alias_in_progress = []
                            continue
                        elif token_in_progress in self.valid_keywords: # New command started
                            allowVars = False
                            tokens.append(" ".join(alias_in_progress))
                            alias_in_progress = []
                        else:   # Add token to alias buffer
                            alias_in_progress.append(token_in_progress)
                            token_in_progress = ''
                            continue
                    if token_in_progress == "from": # Alias enabled zone
                        allowVars = True
                    if token_in_progress == "join":
                        allowVars = True
                    tokens.append(token_in_progress)
                    token_in_progress = ''
            else: # Build token
                token_in_progress += char
            if token_in_progress == '--': # Ignore commends
                return tokens
 
        return tokens

#####################################
# main():
#   Starts command-line query using BQL.
# args:
#   None.
# return:
#   None.
#####################################
def main():
    #Initialize program
    program = BQLBase()
    command = ""
    database = bdb.DatabaseHandler()
    print("# Basic Querying Language Initiated.")
    command_made = True

    while (True):
        # User prompt
        if command_made:
            print("# ", end = "")
            command_made = False

        # Test for input through users and redirects
        try:
            userInput = input().lower()
            command += userInput.strip()
        except:
            break

        # Test for comment lines
        if len(command) >= 2 and command[:2] == "--":
            command = ""
            continue

        # Test for special commands
        if command == ".exit":
            command = ""
            break
        elif command.strip() == "":
            command_made = True
            print()
            continue

        # Test for complete commands.
        #if command[-1] == ';':
        if ";" in command:
            command_made = True
            query_tokens = program.tokenize(command)
            if query_tokens != []: # Run complete command
                program.runCommand(database, query_tokens)
            command = ""
        else: # If not complete, append to current command
            command += " "

    print("\nAll done.")


if __name__ == '__main__':
    main()
