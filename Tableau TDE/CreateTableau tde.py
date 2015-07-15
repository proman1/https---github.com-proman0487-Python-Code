import os, time, pymssql
import dataextract as tde

start_time = time.time()
dotsevery = 75

tdefilename = '' # Tableau File Name
sql = "" # SQL Query
sqlserverhost = '' # Server Name
sqlusername = '' #Username
sqlpassword = '' #password
sqldatabase = '' # Database Name
rowoutput = False

mssql_db = pymssql.connect(host=sqlserverhost, user=sqlusername, password=sqlpassword, database=sqldatabase, as_dict=True) # as_dict very important
mssql_cursor = mssql_db.cursor()
mssql_cursor.execute(sql)

fieldnamelist = []

for row in mssql_cursor:
    itemz = len(row.keys())/2
    for k in row.keys():
        fieldnamelist.append(str(k) + '|' + str(type(row[k])).replace("<type '","").replace("'>","").replace("<class '","").replace('NoneType','str').replace('uuid.UUID','str') )
        break
fieldnamelist.sort()
del fieldnamelist[0:itemz]
    
# create THe extract file
try:
    tdefile = tde.Extract(tdefilename)
except:
    os.system('del' +tdefilename)
    tdefile = tde.Extract(tdefilename)

# create tabledef

tableDef = tde.TableDefinition()
if rowoutput == True:
    print '***** field name list ****'
for t in fieldnamelist:
    fieldtype = t.split('|')[1]
    fieldname = t.split('|')[0]
    fieldtype = str(fieldtype).replace("str","15").replace("datetime.datetime","13").replace("int","7").replace("decimal.Decimal","10").replace("float","10").replace("uuid.UUID","15").replace("bool","11")
    if rowoutput == True:
        print fieldname + '  (looks like ' + t.split('|')[1] +', TDE datatype ' + fieldtype + ')'  # debug
    try:
        tableDef.addColumn(fieldname, int(fieldtype))
    except:
        tableDef.addColumn(fieldname, 15)
if rowoutput == True:
    print '***'
    time.sleep(5)

if rowoutput == True:
    print '################## TDE table definition created ######################'
    for c in range(0,tableDef.getColumnCount()):
        print 'Column: ' + str(tableDef.getColumnName(c)) + ' Type: ' + str(tableDef.getColumnType(c))
    time.sleep(5)

#Step 3
table = tdefile.addTable('Extract',tableDef)
rowsinserted = 1

for row in curr:
    if rowoutput == True:
        print '************** INSERTING ROW NUMBER: ' + str(rowsinserted) + '**************' 
    else:
        if (rowsinserted%dotsevery) == 0:
            print '.',
    columnposition = 0
    newrow = tde.Row(tableDef)

    for t in fieldnamelist:
        fieldtype = t.split('|')[1]
        fieldname = t.split('|')[0]

        if rowoutput == True:
            print str(columnposition) + ' ' + fieldname + ':   ' + str(row[fieldname]) + ' (' + str(fieldtype).split('.')[0] + ')' # debug output
        
        if fieldtype == 'str':
            if row[fieldname] != None:
                newrow.setCharString(columnposition, str(row[fieldname]))
            else:
                newrow.setNull(columnposition)

        if fieldtype == 'int':
            if row[fieldname] != None:
                newrow.setInteger(columnposition, row[fieldname])
            else:
                newrow.setNull(columnposition)

        if fieldtype == 'bool':
            if row[fieldname] != None:
                newrow.setBoolean(columnposition, row[fieldname])
            else:
                newrow.setNull(columnposition)

        if fieldtype == 'decimal.Decimal':
            if row[fieldname] != None:
                newrow.setDouble(columnposition, row[fieldname])
            else:
                newrow.setNull(columnposition)

        if fieldtype == 'datetime.datetime': 
            if row[fieldname] != None:
                strippeddate = str(row[fieldname]).split('.')[0] 
                timechunks = time.strptime(str(strippeddate), "%Y-%m-%d %H:%M:%S")
                newrow.setDateTime(columnposition, timechunks[0], timechunks[1], timechunks[2], timechunks[3], timechunks[4], timechunks[5], 0000)
            else:
                newrow.setNull(columnposition)
    
        columnposition = columnposition + 1 !
    tabletran.insert(newrow)
    newrow.close()
    rowsinserted = rowsinserted + 1

tdefile.close()
conn.close()
timetaken = time.time() - start_time
print str(rowsinserted) + ' rows inserted in ' + str(timetaken) + ' seconds'
print '    (' + str(rowsinserted/timetaken) + ' rows per second)'

