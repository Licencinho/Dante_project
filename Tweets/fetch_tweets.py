#!/usr/bin/env python
from twython import Twython
import string, json, pprint
import urllib
from datetime import timedelta
from datetime import date
from time import *
import sys
import string, os, sys, subprocess, time
# un monton de imports que creo dan lo mismo
from datetime import datetime, timedelta
from email.utils import parsedate_tz
import MySQLdb
APP_KEY="pdcRNVsBbj4iAJTVxBxOw"
APP_SECRET="ID9kHOqig6xetE4bDxMWCVFUn4lfrhcE4XTsSmOhw"
OAUTH_TOKEN="102881645-WxqKJwl5prMQfvn6mTQYFjcUccINMQ51njY0U5Wb"
OAUTH_TOKEN_SECRET="NI14KK4EEcLnq5GpYq0OKdLbGAqdvvHjF9kzhOoHRk"
#objeto Twitter
twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)


conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="test")
cur = conn.cursor()
# conectar a base de datos y crear cursor

harvest_list = ['longueira', 'lavin', 'matthei']
#elementos a buscar en twitter, queda cada uno sparado en la base de datos


cur.execute("select max(BatchId) from TweetLog")
batch_id_cur = cur.fetchall()
batch_id = int(batch_id_cur[0][0] or 0)+1
# Obtengo el utlimo proceso y lo incremento en 1 para saber la cantidad que he ejecutado


for tweet_keyword in sys.argv[1:]: # for each keyword, do some shit

        cur.execute("""delete from TweetBankTemp where tweet_keyword = '"""+str(tweet_keyword)+"""'""")
        conn.commit()
        # whack the temp table in case we didn't exit cleanly

        search_results = twitter.search(q=tweet_keyword, count=100)
        # our search for the current keyword

        #pp = pprint.PrettyPrinter(indent=3)
        # uncomment for debugging and displaying pretty output

        for tweet in search_results["statuses"]:
		ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
		print ts
		print tweet['user']['utc_offset']
	 	# some me the tweet, jerry!
                print "        Tweet from @%s Date: %s" % (tweet['user']['screen_name'].encode('utf-8'),tweet['created_at'])
                print "        ",tweet['text'].encode('utf-8'),"\n"
		print " " 

                try:
                        # lets try to to put each tweet in our temp table for now
                        cur.execute("""insert into TweetBankTemp (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang, offset)
                                        values ('"""+str(tweet['id_str'].encode('utf-8').replace("'","''").replace(';',''))+"""',
        					'"""+ts+"""',                                       
	                                        '"""+str(tweet_keyword)+"""',
                                                '"""+str(tweet['text'].encode('utf-8').replace("'","''").replace(';',''))+"""',
                                                '"""+str(tweet['user']['screen_name'].encode('utf-8').replace("'","''").replace(';',''))+"""',
                                                '"""+str(tweet['lang'].encode('utf-8').replace("'","''").replace(';',''))+"""',
                                                '"""+str(tweet['user']['utc_offset'])+"""'
                                        ) """)
			conn.commit()
                except:
                        print "############### Unexpected error:", sys.exc_info()[0], "##################################"
                        # Si falla algo, atraparlo para no matar todo el proceso


        cur.execute("""insert into TweetBank (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang, offset)
        select * from TweetBankTemp where tweet_id NOT in
        (select distinct tweet_id from TweetBank)""")
        # poner todos los tweet nuevos en la base de datos total, evitando tener repetidos los id
	conn.commit()
        cur.execute("""delete from TweetBankTemp where tweet_keyword = '"""+str(tweet_keyword)+"""'""")
        # take all THESE out of the temp table to not
        # interfere with the next keyword
        #obtengo el total de tweets obtenidos
        cur.execute("""select count(1) from TweetBank where tweet_keyword = '"""+str(tweet_keyword)+"""'""")
        t=cur.fetchall()
        total=int(t[0][0] or 0)
        cur.execute("""select TotalHarvested from TweetLog where keyword = '"""+str(tweet_keyword)+"""' limit 1""")
        tb=cur.fetchall()
        thisbatch=int(tb[0][0] or 0)
        cur.execute("""insert into TweetLog (BatchId, keyword, RunDate, HarvestedThisRun, TotalHarvested) values
        (
        '"""+str(batch_id)+"""',
        '"""+str(tweet_keyword)+"""',
        curdate(),
        '"""+(int(int(total)-int(thisbatch)))+"""',
        (select count(1) from TweetBank where tweet_keyword = '"""+str(tweet_keyword)+"""')
        )""")
        # agregar record a la tabla de log para recoradar que se hizo

        conn.commit()

