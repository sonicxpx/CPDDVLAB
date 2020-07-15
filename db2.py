#
# Set up Jupyter MAGIC commands "sql". 
# %sql will return results from a DB2 select statement or execute a DB2 command
#
# IBM 2020: George Baklarz
# Version 2020-05-18
#

from __future__ import print_function

import json
import os
import sys
import re
import warnings
import string

_settings = {
     "format"   : "pandas",
     "delim"    : ";",
     "quotes"   : True,
     "database" : "",
     "hostname" : "localhost",
     "port"     : "50000",
     "protocol" : "TCPIP",    
     "uid"      : "DB2INST1",
     "pwd"      : "password",
     "ssl"      : "",
     "pandas"   : False
}

# Determine if we can use Pandas dataframes for result sets

try:
    import pandas
    _settings['format'] = 'pandas'
    _settings['pandas'] = True
except:
    _settings['format'] = 'array'
    _settings['pandas'] = False

# Check to see if Db2 libraries exist

try:
    import ibm_db
    import ibm_db_dbi
except:
    print("Unable to load Db2 libraries: ibm_db, ibm_db_dbi. These modules are required to access Db2")
    sys.exit(0)

    
def split_string(in_port,splitter=":"):
 
    # Split input into an IP address and Port number
    
    global _settings

    checkports = in_port.split(splitter)
    ip = checkports[0]
    if (len(checkports) > 1):
        port = checkports[1]
    else:
        port = None

    return ip, port    

#-----------------------------------------------------------
# Connection Parser 
#-----------------------------------------------------------

def parseConnect(inSQL,local_ns):
    
    global _settings, _connected

    _connected = False

    _, allSQL = sqlParser(inSQL,local_ns)
    
    cParms = allSQL.split()
    cnt = 0
    
    _settings["ssl"] = ""
    
    while cnt < len(cParms):
        if cParms[cnt].upper() == 'TO':
            if cnt+1 < len(cParms):
                _settings["database"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No database specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == "SSL":
            _settings["ssl"] = "Security=SSL;"  
            cnt = cnt + 1
        elif cParms[cnt].upper() == 'USER':
            if cnt+1 < len(cParms):
                _settings["uid"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No userid specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'USING':
            if cnt+1 < len(cParms):
                _settings["pwd"] = cParms[cnt+1]   
                cnt = cnt + 1
            else:
                errormsg("No password specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'HOST':
            if cnt+1 < len(cParms):
                _settings["hostname"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No hostname specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'PORT':                           
            if cnt+1 < len(cParms):
                _settings["port"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No port specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() in ('CLOSE','RESET') :
            try:
                result = ibm_db.close(_hdbc)
                _hdbi.close()
            except:
                pass
                     
            if cParms[cnt].upper() == 'RESET': 
                _settings["database"] = ''
            return
        else:
            cnt = cnt + 1
                     
    _ = db2_doConnect()            
    
#-----------------------------------------------------------
# Connect to Db2 
#-----------------------------------------------------------

def db2_doConnect():
    
    global _hdbc, _hdbi, _connected
    global _settings  

    if _connected == False: 
        
        if len(_settings["database"]) == 0:
            return False

    dsn = (
           "DRIVER={{IBM DB2 ODBC DRIVER}};"
           "DATABASE={0};"
           "HOSTNAME={1};"
           "PORT={2};"
           "PROTOCOL=TCPIP;"
           "UID={3};"
           "PWD={4};{5}").format(_settings["database"], 
                                 _settings["hostname"], 
                                 _settings["port"], 
                                 _settings["uid"], 
                                 _settings["pwd"],
                                 _settings["ssl"])

    # Get a database handle (hdbc) and a statement handle (hstmt) for subsequent access to DB2

    try:
        _hdbc  = ibm_db.connect(dsn, "", "")
    except Exception as err:
        db2_error() # errormsg(str(err))
        _connected = False
        _settings["database"] = ''
        return False
    
    try:
        _hdbi = ibm_db_dbi.Connection(_hdbc)
    except Exception as err:
        db2_error() # errormsg(str(err))
        _connected = False
        _settings["database"] = ''
        return False  
    
    _connected = True
    
    errormsg("Connection successful.",0,"00000")

    return True
    
#------------------------------
# Error Message Handling
#------------------------------

def db2_error():
    
    global _sqlerror, _sqlcode, _sqlstate, _connected
    
    _sqlerror = "No error text available"
    _sqlcode = -99999
    _sqlstate = "-99999"  
    
    try:
        if (_connected == True):
            errmsg = ibm_db.stmt_errormsg().replace('\r',' ')
            errmsg = errmsg[errmsg.rfind("]")+1:].strip()
        else:
            errmsg = ibm_db.conn_errormsg().replace('\r',' ')
            errmsg = errmsg[errmsg.rfind("]")+1:].strip()
            
        _sqlerror = errmsg
 
        msg_start = errmsg.find("SQLSTATE=")
        if (msg_start != -1):
            msg_end = errmsg.find(" ",msg_start)
            if (msg_end == -1):
                msg_end = len(errmsg)
            _sqlstate = errmsg[msg_start+9:msg_end]
        else:
            _sqlstate = "0"
    
        msg_start = errmsg.find("SQLCODE=")
        if (msg_start != -1):
            msg_end = errmsg.find(" ",msg_start)
            if (msg_end == -1):
                msg_end = len(errmsg)
            _sqlcode = errmsg[msg_start+8:msg_end]
            try:
                _sqlcode = int(_sqlcode)
            except:
                pass
        else:        
            _sqlcode = 0
            
    except:
        errmsg = "Unknown error."
        _sqlcode = -99999
        _sqlstate = "-99999"
        _sqlerror = errmsg
        return
        
    
    msg_start = errmsg.find("SQLSTATE=")
    if (msg_start != -1):
        msg_end = errmsg.find(" ",msg_start)
        if (msg_end == -1):
            msg_end = len(errmsg)
        _sqlstate = errmsg[msg_start+9:msg_end]
    else:
        _sqlstate = "0"
        
    
    msg_start = errmsg.find("SQLCODE=")
    if (msg_start != -1):
        msg_end = errmsg.find(" ",msg_start)
        if (msg_end == -1):
            msg_end = len(errmsg)
        _sqlcode = errmsg[msg_start+8:msg_end]
        try:
            _sqlcode = int(_sqlcode)
        except:
            pass
    else:
        _sqlcode = 0
    
#------------------------------
# Error message
#------------------------------

def errormsg(sqlerror, sqlcode=-99999, sqlstate="-99999"):

    global _sqlcode, _sqlstate, _sqlerror
    
    _sqlerror = sqlerror
    _sqlcode = sqlcode
    _sqlstate = sqlstate

#------------------------------
# SQL Error Retrieval
#------------------------------   

def sqlcode(request=None):
 
    global _sqlstate, _sqlerror, _sqlcode
    
    if (request == None):
        return _sqlcode
    elif (request == "message"):
        return _sqlerror
    elif (request == "sqlstate"):
        return _sqlstate
    elif (request == "sqlcode"):
        return _sqlcode
    else:
        return _sqlcode
        

#------------------------------
# SQL Parser 
#------------------------------

def sqlParser(sqlin,local_ns):

    global _settings
       
    sql_cmd = ""
    encoded_sql = sqlin
    
    firstCommand = "(?:^\s*)([a-zA-Z]+)(?:\s+.*|$)"
    
    findFirst = re.match(firstCommand,sqlin)
    
    if (findFirst == None): # We did not find a match so we just return the empty string
        return sql_cmd, encoded_sql
    
    cmd = findFirst.group(1)
    sql_cmd = cmd.upper()

    #
    # Scan the input string looking for variables in the format :var. If no : is found just return.
    # Var must be alpha+number+_ to be valid
    #
    
    if (':' not in sqlin): # A quick check to see if parameters are in here, but not fool-proof!         
        return sql_cmd, encoded_sql    
    
    inVar = False 
    inQuote = "" 
    varName = ""
    encoded_sql = ""
    
    STRING = 0
    NUMBER = 1
    LIST = 2
    RAW = 3

    flag_quotes = _settings["quotes"]
    
    for ch in sqlin:
        if (inVar == True): # We are collecting the name of a variable
            if (ch.upper() in "@_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"):
                varName = varName + ch
                continue
            else:
                if (varName == ""):
                    encoded_sql = encoded_sql + ":"
                else:
                    varValue, varType = getContents(varName,flag_quotes,local_ns)
                    if (varValue == None):                 
                        encoded_sql = encoded_sql + ":" + varName
                    else:
                        if (varType == STRING):
                            encoded_sql = encoded_sql + varValue
                        elif (varType == NUMBER):
                            encoded_sql = encoded_sql + str(varValue)
                        elif (varType == RAW):
                            encoded_sql = encoded_sql + varValue
                        elif (varType == LIST):
                            start = True
                            for v in varValue:
                                if (start == False):
                                    encoded_sql = encoded_sql + ","
                                if (isinstance(v,int) == True):         # Integer value 
                                    encoded_sql = encoded_sql + str(v)
                                elif (isinstance(v,float) == True):
                                    encoded_sql = encoded_sql + str(v)
                                else:
                                    flag_quotes = True
                                    try:
                                        if (v.find('0x') == 0):               # Just guessing this is a hex value at beginning
                                            encoded_sql = encoded_sql + v
                                        else:
                                            encoded_sql = encoded_sql + addquotes(v,flag_quotes)      # String
                                    except:
                                        encoded_sql = encoded_sql + addquotes(str(v),flag_quotes)                                   
                                start = False

                encoded_sql = encoded_sql + ch
                varName = ""
                inVar = False  
        elif (inQuote != ""):
            encoded_sql = encoded_sql + ch
            if (ch == inQuote): inQuote = ""
        elif (ch in ("'",'"')):
            encoded_sql = encoded_sql + ch
            inQuote = ch
        elif (ch == ":"): # This might be a variable
            varName = ""
            inVar = True
        else:
            encoded_sql = encoded_sql + ch
    
    if (inVar == True):
        varValue, varType = getContents(varName,flag_quotes,local_ns) # We assume the end of a line is quoted
        if (varValue == None):                 
            encoded_sql = encoded_sql + ":" + varName  
        else:
            if (varType == STRING):
                encoded_sql = encoded_sql + varValue
            elif (varType == NUMBER):
                encoded_sql = encoded_sql + str(varValue)
            elif (varType == LIST):
                flag_quotes = True
                start = True
                for v in varValue:
                    if (start == False):
                        encoded_sql = encoded_sql + ","
                    if (isinstance(v,int) == True):         # Integer value 
                        encoded_sql = encoded_sql + str(v)
                    elif (isinstance(v,float) == True):
                        encoded_sql = encoded_sql + str(v)
                    else:
                        try:
                            if (v.find('0x') == 0):               # Just guessing this is a hex value
                                encoded_sql = encoded_sql + v
                            else:
                                encoded_sql = encoded_sql + addquotes(v,flag_quotes)              # String
                        except:
                            encoded_sql = encoded_sql + addquotes(str(v),flag_quotes)                                 
                    start = False

    return sql_cmd, encoded_sql

#------------------------------
#  Find a local variable
#------------------------------

def getLocal(varName, local_ns):

    value = None 
    for localVar, localValue in local_ns.items():
        if (localVar == varName):
            value = localValue
            break

    return value

#------------------------------
# Get contents of a variable
#------------------------------

def getContents(varName,flag_quotes,local_ns):
    
    #
    # Get the contents of the variable name that is passed to the routine. Only simple
    # variables are checked, i.e. arrays and lists are not parsed
    #
    
    STRING = 0
    NUMBER = 1
    LIST = 2
    RAW = 3
    DICT = 4
    
    # See if we can find the variable

    value = getLocal(varName, local_ns)
      
    if (value == None):
        return(None,STRING)
    
    if (isinstance(value,dict) == True):          # Check to see if this is JSON dictionary
        return(addquotes(value,flag_quotes),STRING)

    elif(isinstance(value,list) == True):         # List - tricky 
        return(value,LIST)

    elif (isinstance(value,int) == True):         # Integer value 
        return(value,NUMBER)

    elif (isinstance(value,float) == True):       # Float value
        return(value,NUMBER)

    else:
        try:
            # The pattern needs to be in the first position (0 in Python terms)
            if (value.find('0x') == 0):               # Just guessing this is a hex value
                return(value,RAW)
            else:
                return(addquotes(value,flag_quotes),STRING)                     # String
        except:
            return(addquotes(str(value),flag_quotes),RAW)

#------------------------------
# Add Quotes to variable
#------------------------------

def addquotes(inString,flag_quotes):

    global _settings
    
    if (isinstance(inString,dict) == True):          # Check to see if this is JSON dictionary
        serialized = json.dumps(inString) 
    else:
        serialized = inString

    # Replace single quotes with '' (two quotes) and wrap everything in single quotes
    if (flag_quotes == False):
        return(serialized)
    else:
        return("'"+serialized.replace("'","''")+"'")    # Convert single quotes to two single quotes


#------------------------------
# Find a procedure
#------------------------------

def findProc(procname):
    
    global _hdbc, _hdbi, _connected
    
    # Split the procedure name into schema.procname if appropriate
    upper_procname = procname.upper()
    schema, proc = split_string(upper_procname,".") # Expect schema.procname
    if (proc == None):
        proc = schema

    # Call ibm_db.procedures to see if the procedure does exist
    schema = "%"

    try:
        stmt = ibm_db.procedures(_hdbc, None, schema, proc) 
        if (stmt == False):                         # Error executing the code
            errormsg("Procedure " + procname + " not found in the system catalog.")
            return None

        result = ibm_db.fetch_tuple(stmt)
        resultsets = result[5]
        if (resultsets >= 1): resultsets = 1
        return resultsets
            
    except Exception as err:
        errormsg("Procedure " + procname + " not found in the system catalog.")
        return None
        
#------------------------------
# Parse Call Arguments
#------------------------------

def parseCallArgs(macro):
    
    quoteChar = ""
    inQuote = False
    inParm = False
    ignore = False
    name = ""
    parms = []
    parm = ''
    
    sqlin = macro.replace("\n","")
    sqlin.lstrip()
    
    for ch in sqlin:
        if (inParm == False):
            # We hit a blank in the name, so ignore everything after the procedure name until a ( is found
            if (ch == " "): 
                ignore == True
            elif (ch ==  "("): # Now we have parameters to send to the stored procedure
                inParm = True
            else:
                if (ignore == False): name = name + ch # The name of the procedure (and no blanks)
        else:
            if (inQuote == True):
                if (ch == quoteChar):
                    inQuote = False  
                else:
                    parm = parm + ch
            elif (ch in ("\"","\'","[")): # Do we have a quote
                if (ch == "["):
                    quoteChar = "]"
                else:
                    quoteChar = ch
                inQuote = True
            elif (ch == ")"):
                if (parm != ""):
                    parms.append(parm)
                parm = ""
                break
            elif (ch == ","):
                if (parm != ""):
                    parms.append(parm)                  
                else:
                    parms.append("null")
                    
                parm = ""

            else:
                parm = parm + ch
                
    if (inParm == True):
        if (parm != ""):
            parms.append(parm)    
            
    return(name,parms)

#------------------------------
# Get columns 
#------------------------------

def getColumns(stmt):
    
    columns = []
    types = []
    colcount = 0
    try:
        colname = ibm_db.field_name(stmt,colcount)
        coltype = ibm_db.field_type(stmt,colcount)
        while (colname != False):
            columns.append(colname)
            types.append(coltype)
            colcount += 1
            colname = ibm_db.field_name(stmt,colcount)
            coltype = ibm_db.field_type(stmt,colcount)            
        return columns,types   
                
    except Exception as err:
        db2_error()
        return None

#------------------------------
# Call a procedure
#------------------------------

def parseCall(hdbc, inSQL, local_ns):
    
    global _hdbc, _hdbi, _connected, _settings
    
    # Check to see if we are connected first
    if (_connected == False):                                      # Check if you are connected 
        db2_doConnect()
        if _connected == False: return None

    flag_quotes = _settings["quotes"]
    
    remainder = inSQL.strip()
    procName, procArgs = parseCallArgs(remainder[5:]) # Assume that CALL ... is the format
    
    resultsets = findProc(procName)
    if (resultsets == None): return None
    
    argvalues = []
 
    if (len(procArgs) > 0): # We have arguments to consider
        for arg in procArgs:
            varname = arg
            if (len(varname) > 0):
                if (varname[0] == ":"):
                    checkvar = varname[1:]
                    varvalue = getContents(checkvar,flag_quotes,local_ns)
                    if (varvalue == None):
                        errormsg("Variable " + checkvar + " is not defined.")
                        return None
                    argvalues.append(varvalue)
                else:
                    if (varname.upper() == "NULL"):
                        argvalues.append(None)
                    else:
                        argvalues.append(varname)
            else:
                argvalues.append(None)

    
    try:

        if (len(procArgs) > 0):
            argtuple = tuple(argvalues)
            result = ibm_db.callproc(_hdbc,procName,argtuple)
            stmt = result[0]
        else:
            result = ibm_db.callproc(_hdbc,procName)
            stmt = result
        
        if (resultsets != 0 and stmt != None): 

            columns, types = getColumns(stmt)
            if (columns == None): return None
            
            rows = []
            rowlist = ibm_db.fetch_tuple(stmt)
            while ( rowlist ) :
                row = []
                colcount = 0
                for col in rowlist:
                    try:
                        if (types[colcount] in ["int","bigint"]):
                            row.append(int(col))
                        elif (types[colcount] in ["decimal","real"]):
                            row.append(float(col))
                        elif (types[colcount] in ["date","time","timestamp"]):
                            row.append(str(col))
                        else:
                            row.append(col)
                    except:
                        row.append(col)
                    colcount += 1
                rows.append(row)
                rowlist = ibm_db.fetch_tuple(stmt)
            
            if (_settings["format"] == "array"):
                rows.insert(0,columns)
                if len(procArgs) > 0:
                    allresults = []
                    allresults.append(rows)
                    for x in result[1:]:
                        allresults.append(x)
                    return allresults # rows,returned_results
                else:
                    return rows
            else:
                df = pandas.DataFrame.from_records(rows,columns=columns)
                return df
            
        else:
            if len(procArgs) > 0:
                allresults = []
                for x in result[1:]:
                    allresults.append(x)
                return allresults # rows,returned_results
            else:
                return None
            
    except Exception as err:
        db2_error()
        return None

#-------------------------------
#  Split arguments
#-------------------------------        

def splitargs(arguments):
    
    import types
    
    # String the string and remove the ( and ) characters if they at the beginning and end of the string
    
    results = []
    
    step1 = arguments.strip()
    if (len(step1) == 0): return(results)       # Not much to do here - no args found
    
    if (step1[0] == '('):
        if (step1[-1:] == ')'):
            step2 = step1[1:-1]
            step2 = step2.strip()
        else:
            step2 = step1
    else:
        step2 = step1
            
    # Now we have a string without brackets. Start scanning for commas
            
    quoteCH = ""
    pos = 0
    arg = ""
    args = []
            
    while pos < len(step2):
        ch = step2[pos]
        if (quoteCH == ""):                     # Are we in a quote?
            if (ch in ('"',"'")):               # Check to see if we are starting a quote
                quoteCH = ch
                arg = arg + ch
                pos += 1
            elif (ch == ","):                   # Are we at the end of a parameter?
                arg = arg.strip()
                args.append(arg)
                arg = ""
                inarg = False 
                pos += 1
            else:                               # Continue collecting the string
                arg = arg + ch
                pos += 1
        else:
            if (ch == quoteCH):                 # Are we at the end of a quote?
                arg = arg + ch                  # Add the quote to the string
                pos += 1                        # Increment past the quote
                quoteCH = ""                    # Stop quote checking (maybe!)
            else:
                pos += 1
                arg = arg + ch

    if (quoteCH != ""):                         # So we didn't end our string
        arg = arg.strip()
        args.append(arg)
    elif (arg != ""):                           # Something left over as an argument
        arg = arg.strip()
        args.append(arg)
    else:
        pass
    
    results = []
    
    for arg in args:
        result = []
        if (len(arg) > 0):
            if (arg[0] in ('"',"'")):
                value = arg[1:-1]
                isString = True
                isNumber = False
            else:
                isString = False 
                isNumber = False 
                try:
                    value = eval(arg)
                    if (type(value) == int):
                        isNumber = True
                    elif (isinstance(value,float) == True):
                        isNumber = True
                    else:
                        value = arg
                except:
                    value = arg

        else:
            value = ""
            isString = False
            isNumber = False
            
        result = [value,isString,isNumber]
        results.append(result)
        
    return results        

#------------------------------
# Prepare/Execute 
#------------------------------

def parsePExec(hdbc, inSQL):
     
    import ibm_db    
    global _stmt, _stmtID, _stmtSQL, _sqlcode
    
    cParms = inSQL.split()
    parmCount = len(cParms)
    if (parmCount == 0): return(None)                          # Nothing to do but this shouldn't happen
    
    keyword = cParms[0].upper()                                  # Upper case the keyword
    
    if (keyword == "PREPARE"):                                   # Prepare the following SQL
        uSQL = inSQL.upper()
        found = uSQL.find("PREPARE")
        sql = inSQL[found+7:].strip()

        try:
            pattern = "\?\*[0-9]+"
            findparm = re.search(pattern,sql)
            while findparm != None:
                found = findparm.group(0)
                count = int(found[2:])
                markers = ('?,' * count)[:-1]
                sql = sql.replace(found,markers)
                findparm = re.search(pattern,sql)
            
            stmt = ibm_db.prepare(hdbc,sql) # Check error code here
            if (stmt == False): 
                db2_error()
                return(False)
            
            stmttext = str(stmt).strip()
            stmtID = stmttext[33:48].strip()
            
            if (stmtID in _stmtID) == False:
                _stmt.append(stmt)              # Prepare and return STMT to caller
                _stmtID.append(stmtID)
            else:
                stmtIX = _stmtID.index(stmtID)
                _stmt[stmtIX] = stmt
                 
            return(stmtID)
        
        except Exception as err:
            db2_error()
            return(False)

    if (keyword == "EXECUTE"):                                  # Execute the prepare statement
        if (parmCount < 2): return(False)                    # No stmtID available
        
        stmtID = cParms[1].strip()
        if (stmtID in _stmtID) == False:
            errormsg("Prepared statement not found or invalid.")
            return(False)

        stmtIX = _stmtID.index(stmtID)
        stmt = _stmt[stmtIX]

        try:        

            if (parmCount == 2):                           # Only the statement handle available
                result = ibm_db.execute(stmt)               # Run it
            elif (parmCount == 3):                          # Not quite enough arguments
                errormsg("Missing or invalid USING clause on EXECUTE statement.")
                _sqlcode = -99999
                return(False)
            else:
                using = cParms[2].upper()
                if (using != "USING"):                     # Bad syntax again
                    errormsg("Missing USING clause on EXECUTE statement.")
                    _sqlcode = -99999
                    return(False)
                
                uSQL = inSQL.upper()
                found = uSQL.find("USING")
                parmString = inSQL[found+5:].strip()
                parmset = splitargs(parmString)
 
                if (len(parmset) == 0):
                    errormsg("Missing parameters after the USING clause.")
                    _sqlcode = -99999
                    return(False)
                    
                parms = []

                parm_count = 0
                
                CONSTANT = 0
                VARIABLE = 1
                const = [0]
                const_cnt = 0
                
                for v in parmset:
                    
                    parm_count = parm_count + 1
                    
                    if (v[1] == True or v[2] == True): # v[1] true if string, v[2] true if num
                        
                        parm_type = CONSTANT                        
                        const_cnt = const_cnt + 1
                        if (v[2] == True):
                            if (isinstance(v[0],int) == True):         # Integer value 
                                sql_type = ibm_db.SQL_INTEGER
                            elif (isinstance(v[0],float) == True):       # Float value
                                sql_type = ibm_db.SQL_DOUBLE
                            else:
                                sql_type = ibm_db.SQL_INTEGER
                        else:
                            sql_type = ibm_db.SQL_CHAR
                        
                        const.append(v[0])

                        
                    else:
                    
                        parm_type = VARIABLE
                    
                        # See if the variable has a type associated with it varname@type
                    
                        varset = v[0].split("@")
                        parm_name = varset[0]
                        
                        parm_datatype = "char"

                        # Does the variable exist?
                        if (parm_name not in globals()):
                            errormsg("SQL Execute parameter " + parm_name + " not found")
                            _sqlcode = -99999
                            return(False)                        
        
                        if (len(varset) > 1):                # Type provided
                            parm_datatype = varset[1]

                        if (parm_datatype == "dec" or parm_datatype == "decimal"):
                            sql_type = ibm_db.SQL_DOUBLE
                        elif (parm_datatype == "bin" or parm_datatype == "binary"):
                            sql_type = ibm_db.SQL_BINARY
                        elif (parm_datatype == "int" or parm_datatype == "integer"):
                            sql_type = ibm_db.SQL_INTEGER
                        else:
                            sql_type = ibm_db.SQL_CHAR
                    
                    try:
                        if (parm_type == VARIABLE):
                            result = ibm_db.bind_param(stmt, parm_count, globals()[parm_name], ibm_db.SQL_PARAM_INPUT, sql_type)
                        else:
                            result = ibm_db.bind_param(stmt, parm_count, const[const_cnt], ibm_db.SQL_PARAM_INPUT, sql_type)
                            
                    except:
                        result = False
                        
                    if (result == False):
                        errormsg("SQL Bind on variable " + parm_name + " failed.")
                        _sqlcode = -99999
                        return(False) 
                    
                result = ibm_db.execute(stmt) # ,tuple(parms))
                
            if (result == False): 
                errormsg("SQL Execute failed.")      
                return(False)
            
            if (ibm_db.num_fields(stmt) == 0): return(True) # Command successfully completed
                          
            return(fetchResults(stmt))
                        
        except Exception as err:
            db2_error()
            return(False)
        
        return(False)
  
    return(False)

#------------------------------
# Fetch Result Sets
#------------------------------

def fetchResults(stmt):
     
    global _sqlcode, _settings
    
    rows = []
    columns, types = getColumns(stmt)
    
    # By default we assume that the data will be an array
    is_array = True
    
    # Check what type of data we want returned - array or json
    if (_settings["format"] == "json"):
        is_array = False
    
    # Set column names to lowercase for JSON records
    if (is_array == False):
        columns = [col.lower() for col in columns] # Convert to lowercase for each of access
    
    # First row of an array has the column names in it
    if (is_array == True):
        rows.append(columns)
        
    result = ibm_db.fetch_tuple(stmt)
    rowcount = 0
    while (result):
        
        rowcount += 1
        
        if (is_array == True):
            row = []
        else:
            row = {}
            
        colcount = 0
        for col in result:
            try:
                if (types[colcount] in ["int","bigint"]):
                    if (is_array == True):
                        row.append(int(col))
                    else:
                        row[columns[colcount]] = int(col)
                elif (types[colcount] in ["decimal","real"]):
                    if (is_array == True):
                        row.append(float(col))
                    else:
                        row[columns[colcount]] = float(col)
                elif (types[colcount] in ["date","time","timestamp"]):
                    if (is_array == True):
                        row.append(str(col))
                    else:
                        row[columns[colcount]] = str(col)
                else:
                    if (is_array == True):
                        row.append(col)
                    else:
                        row[columns[colcount]] = col
                        
            except:
                if (is_array == True):
                    row.append(col)
                else:
                    row[columns[colcount]] = col
                    
            colcount += 1
        
        rows.append(row)
        result = ibm_db.fetch_tuple(stmt)
        
    if (rowcount == 0): 
        _sqlcode = 100        
    else:
        _sqlcode = 0
        
    return rows

#------------------------------
# Pase Commit
#------------------------------

def parseCommit(sql):
    
    global _hdbc, _hdbi, _connected, _stmt, _stmtID, _stmtSQL

    if (_connected == False): return                        # Nothing to do if we are not connected
    
    cParms = sql.split()
    if (len(cParms) == 0): return                           # Nothing to do but this shouldn't happen
    
    keyword = cParms[0].upper()                             # Upper case the keyword
    
    if (keyword == "COMMIT"):                               # Commit the work that was done
        try:
            result = ibm_db.commit (_hdbc)                  # Commit the connection
            if (len(cParms) > 1):
                keyword = cParms[1].upper()
                if (keyword == "HOLD"):
                    return
            
            del _stmt[:]
            del _stmtID[:]

        except Exception as err:
            db2_error()
        
        return
        
    if (keyword == "ROLLBACK"):                             # Rollback the work that was done
        try:
            result = ibm_db.rollback(_hdbc)                  # Rollback the connection
            del _stmt[:]
            del _stmtID[:]            

        except Exception as err:
            db2_error()
        
        return
    
    if (keyword == "AUTOCOMMIT"):                           # Is autocommit on or off
        if (len(cParms) > 1): 
            op = cParms[1].upper()                          # Need ON or OFF value
        else:
            return
        
        try:
            if (op == "OFF"):
                ibm_db.autocommit(_hdbc, False)
            elif (op == "ON"):
                ibm_db.autocommit (_hdbc, True)
            return    
        
        except Exception as err:
            db2_error()
            return 
        
    return

#------------------------------
# Split SQL
#------------------------------

def splitSQL(inputString, delimiter):
     
    pos = 0
    arg = ""
    results = []
    quoteCH = ""
    
    inSQL = inputString.strip()
    if (len(inSQL) == 0): return(results)       # Not much to do here - no args found
            
    while pos < len(inSQL):
        ch = inSQL[pos]
        pos += 1
        if (ch in ('"',"'")):                   # Is this a quote characters?
            arg = arg + ch                      # Keep appending the characters to the current arg
            if (ch == quoteCH):                 # Is this quote character we are in
                quoteCH = ""
            elif (quoteCH == ""):               # Create the quote
                quoteCH = ch
            else:
                None
        elif (quoteCH != ""):                   # Still in a quote
            arg = arg + ch
        elif (ch == delimiter):                 # Is there a delimiter?
            results.append(arg)
            arg = ""
        else:
            arg = arg + ch
            
    if (arg != ""):
        results.append(arg)
        
    return(results)

#------------------------------
#  Settings for runing SQL
#------------------------------

def setOptions(local_ns):

    global _settings

    if (_settings["pandas"] == True):
        _settings["format"] = "pandas"
    else:
        _settings["format"] = "array"

    _settings["delim"] = ";"
    _settings["quotes"] = True

    value = getLocal("format",local_ns)

    if (value != None):
        if (value == "pandas"):
            if (_settings["pandas"] == True):
                _settings["format"] = "pandas"
            else:
                print("PANDAS results format unavailable due to PANDAS libraries not loaded")
        elif (value == "array"):
            _settings["format"] = "array"
        elif (value == "json"):
            _settings["format"] = "json"
        else:
            print("Unknown FORMAT option: " + value)

    value = getLocal("delim",local_ns)

    if (value != None):
        _settings["delim"] = value

    value = getLocal("quotes",local_ns)

    if (value == False):
        _settings["quotes"] = False

    return    
#------------------------------
# Main SQL Code
#------------------------------

def sql(sqlstmts=None,**local_ns):
        
    # Before we event get started, check to see if you have connected yet. Without a connection we 
    # can't do anything. You may have a connection request in the code, so if that is true, we run those,
    # otherwise we connect immediately
    
    # If your statement is not a connect, and you haven't connected, we need to do it for you

    global _settings
    global _hdbc, _hdbi, _connected, _sqlstate, _sqlerror, _sqlcode
         
    flag_output = False
    _sqlstate = "0"
    _sqlerror = ""
    _sqlcode = 0

    if (sqlstmts == None): return

    setOptions(local_ns)
 
    sqlstmts = sqlstmts.strip()
    
    if (len(sqlstmts) == 0): return                               # Nothing to do here
            
    sqlType,remainder = sqlParser(sqlstmts,local_ns)              # What type of command do you have?
            
    if (sqlType == "CONNECT"):                                # A connect request 
        parseConnect(sqlstmts,local_ns)
        return
    elif (sqlType == 'COMMIT' or sqlType == 'ROLLBACK' or sqlType == 'AUTOCOMMIT'):
        parseCommit(remainder)
        return
    elif (sqlType == "PREPARE"):
        pstmt = parsePExec(_hdbc, remainder)
        return(pstmt)
    elif (sqlType == "EXECUTE"):
        result = parsePExec(_hdbc, remainder)
        return(result)    
    elif (sqlType == "CALL"):
        result = parseCall(_hdbc, remainder, local_ns)
        return(result)
    else:
        pass        

    if (_connected == False):
        if (db2_doConnect() == False):
            errormsg('A CONNECT statement must be issued before issuing SQL statements.')
            return      
    
    runSQL = re.sub('.*?--.*$',"",sqlstmts,flags=re.M)
    remainder = runSQL.replace("\n"," ") 
    
    sqlLines = splitSQL(remainder,_settings["delim"])

    flag_cell = True
                  
    # For each line figure out if you run it as a command (db2) or select (sql)

    for sqlin in sqlLines:          # Run each command
        
        sqlType, sql = sqlParser(sqlin,local_ns)                           # Parse the SQL  
        if (sql.strip() == ""): continue
            
        try:                                                  # See if we have an answer set
            stmt = ibm_db.prepare(_hdbc,sql)
            if (ibm_db.num_fields(stmt) == 0):                # No, so we just execute the code
                result = ibm_db.execute(stmt)                 # Run it      
                                 
                if (result == False):                         # Error executing the code
                    db2_error() 
                    continue
                    
                rowcount = ibm_db.num_rows(stmt)    
            
                if (rowcount == 0):
                    errormsg("No rows found", 100, "00100")

                continue                                      # Continue running
            
            elif (_settings["format"] == "array" or 
                  _settings["format"] == "json" ):                     # raw, json, format json
                row_count = 0
                resultSet = []
                try:
                    result = ibm_db.execute(stmt)             # Run it
                    if (result == False):                         # Error executing the code
                        db2_error()  
                        return
                        
                    return(fetchResults(stmt))
                          
                except Exception as err:
                    db2_error()
                    return
                    
            else:
                
                try:
                    df = pandas.read_sql(sql,_hdbi)
        
                except Exception as err:
                    db2_error()
                    return
            
                if (len(df) == 0):
                    errormsg("No rows found", 100, "00100")
                    continue                    
            
                flag_output = True
                return df # print(df.to_string())
        
        except:
            db2_error()
            continue # return
                              

#------------------------------
# Startup Script
#------------------------------


warnings.filterwarnings("ignore")

# Connection settings for statements 

_connected = False
_hdbc = None
_hdbi = None
_stmt = []
_stmtID = []
_stmtSQL = []
_vars = {}

# Db2 Error Messages and Codes
_sqlcode = 0
_sqlstate = "0"
_sqlerror = ""
