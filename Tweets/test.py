#!/user/bin/python
#demo.py 
# a simple script to pull some data from MySQL

import MySQLdb
db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="test")
#create a cursor for the select
cur = db.cursor()

#execute an sql query
cur.execute("INSERT INTO testing values ('hola'); ")
db.commit()
##Iterate 
for row in cur.fetchall() :
      #data from rows
        firstname = str(row[0])
      #print 
        print "This Person's name is " + firstname
# close the cursor
cur.close()

# close the connection
db.close ()
