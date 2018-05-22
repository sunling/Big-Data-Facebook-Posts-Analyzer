import os
import re
import json
import pandas as pd
import matplotlib.pyplot as plt
import plotly as py
import plotly.graph_objs as go

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from dateutil import parser
import numpy as np
import time
import fpa_conf

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def getHiveContextInstance(sparkContext):
    if ('hiveContextSingletonInstance' not in globals()):
        globals()['hiveContextSingletonInstance'] = HiveContext(sparkContext)
    return globals()['hiveContextSingletonInstance']

def getSpark():
    if ('sparkhiveSession' not in globals()):
        globals()['sparkhiveSession'] = SparkSession.builder.enableHiveSupport().getOrCreate()
    return globals()['sparkhiveSession'] 

#fig0 count the most liked posts top 5
def countMostLikedPost(hc):
    print("Start countMostLikedPost : %s"+time.ctime())
    likedposts = hc.sql("SELECT message,likes_num FROM t_posts order by likes_num desc limit 5")   
    if likedposts is not None:
        df_likes = likedposts.toPandas()
        #data = go.Data([go.Bar(x=df_likes['message'],y=df_likes['likes'])])
        data = go.Data([go.Table(header=dict(values=['<b>message</b>', '<b>likes</b>'],\
            line = dict(color = '#506784'),\
            fill = dict(color = 'grey'),\
            align = ['left','center'],\
            font = dict(color = 'white', size = 14)),\
            cells=dict(values=(df_likes['message'],df_likes['likes_num']),\
                line = dict(color = '#506784'),\
                fill = dict(color = [['lightgrey','white','lightgrey',\
                                'white','lightgrey']]),\
                align = ['left', 'center'],\
                font = dict(color = '#506784', size = 12)))])

        layout = go.Layout(title='Top 5 most liked posts')
        fig = go.Figure(data=data, layout=layout)
        py.offline.plot(fig, filename="/Users/sunling/MUM/BDT/project/BGFacebook/output/test/"+fpa_conf.user+"_top5_liked_posts.html",auto_open=False)
    print("Start countMostLikedPost : %s"+time.ctime())

def countMostCommentedPost(hc):
    print("Start countMostCommentedPost : %s"+time.ctime())
    comms = hc.sql("SELECT message,comment_count FROM t_posts order by comment_count desc limit 10")   
    comms.show()
    if comms is not None: 
        df_comms = comms.toPandas() 
        data = go.Data([go.Table(header=dict(values=['<b>Post</b>', '<b>Comment_Count</b>'],\
            line = dict(color = '#506784'),\
            fill = dict(color = 'Orange'),\
            align = ['center','center'],\
            font = dict(color = 'white', size = 15)),\
            cells=dict(values=(df_comms['message'],df_comms['comment_count']),\
                line = dict(color = '#506784'),\
                fill = dict(color = [['lightgrey','white','lightgrey',\
                                'white','lightgrey']]),\
                align = ['left', 'center'],\
                font = dict(color = '#506784', size = 12)))])

        layout = go.Layout(title=fpa_conf.user+': Top 10 most Commented posts')
        fig = go.Figure(data=data, layout=layout)
        py.offline.plot(fig, filename="/Users/sunling/MUM/BDT/project/BGFacebook/output/test/"+fpa_conf.user+"_top10_commented_posts.html",auto_open=False)
    print("Start countMostCommentedPost : %s"+time.ctime())


#fig2: count top 10 most used words
def countTop10MostUsedWords(hc): 
    messages = hc.sql("SELECT message FROM t_posts")
    if messages is not None :
        top10words = messages.rdd.flatMap(lambda p:p.message.split(' '))\
            .map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y)\
            .filter(lambda x: x[0]!='')\
            .sortBy(lambda x: x[1],ascending=False)\
            .take(10)

        df = pd.DataFrame(top10words,columns=["word","count"])  
        data = go.Data([go.Pie(labels=df["word"],values=df["count"])])
        layout = go.Layout(title=fpa_conf.user+': Top 10 Words Used')
        fig = go.Figure(data=data, layout=layout)
        py.offline.plot(fig, filename="/Users/sunling/MUM/BDT/project/BGFacebook/output/test/"+fpa_conf.user+"__top10words.html",auto_open=False)

#fig1 :count the number of posts by year 
def countNumberOfPostsByYear(hc): 
    results = hc.sql("SELECT year,count(1) cnt FROM t_posts where year >2000 group by year")
    if results is not None: 
        if results.count() > 5 :
            df_yearcount = results.toPandas()
            data = go.Data([go.Bar(x=df_yearcount['year'],y=df_yearcount['cnt'])])
            layout = go.Layout(title=fpa_conf.user+': Posts of Each Year | Total:'+str(sum(df_yearcount['cnt'])))
            fig = go.Figure(data=data, layout=layout)
            py.offline.plot(fig, filename="/Users/sunling/MUM/BDT/project/BGFacebook/output/test/"+fpa_conf.user+"_CNN_year_count.html",auto_open=False)
        else:
            ym_count = hc.sql("SELECT concat(year,LPAD(month,2,'0')) ym,count(1) cnt from t_posts group by year,month order by ym ")
            ym_count.show()
            df_ymcount = ym_count.toPandas()
            data = go.Data([go.Pie(labels=df_ymcount['ym'],values=df_ymcount['cnt'])])
            layout = go.Layout(title='Posts of Each Month | Total:'+str(sum(df_ymcount['cnt'])))
            fig = go.Figure(data=data, layout=layout)
            py.offline.plot(fig, filename="/Users/sunling/MUM/BDT/project/BGFacebook/output/test/"+fpa_conf.user+"_ym_count.html",auto_open=False)

def createTable(rdd):
    if rdd.count() >0:
        print("rdd.count()>0")
        hc = getHiveContextInstance(rdd.context)
        rowrdd = rdd.map(lambda p: Row(id=p[0], message=p[1],year=parser.parse(p[2]).year,\
            month=parser.parse(p[2]).month,likes_num=p[3],comment_count=p[4]))
        df = hc.createDataFrame(rowrdd)
        df.write.mode('append').saveAsTable('t_posts')
        #analysis1
        countMostLikedPost(hc)
        countMostCommentedPost(hc)
        countNumberOfPostsByYear(hc)
        countTop10MostUsedWords(hc)
        countTop5LikedMostNames(rdd)
        

#fig4 count top 10 person who liked BG's posts most
def countTop5LikedMostNames(rdd):
    if rdd.count()>0 :
        names = rdd.map(lambda r: r[5]).flatMap(lambda arr: [name for name in arr])\
                .map(lambda name : (name,1))\
                .reduceByKey(lambda x,y:(x+y))\
                .sortBy(lambda x: x[1],ascending=False).take(10)
        print(names)
        df = pd.DataFrame(names,columns=["name","count"])  
        data = go.Data([go.Bar(x=df["name"],y=df["count"])])
        layout = go.Layout(title='Top 10 names who liked my posts most')
        fig = go.Figure(data=data, layout=layout)
        py.offline.plot(fig, filename="/Users/sunling/MUM/BDT/project/BGFacebook/output/test/"+fpa_conf.user+"_top10names.html",auto_open=False)

if __name__ == "__main__":

    sc = SparkContext(appName="CS523FinalProject")
    sc.setLogLevel("ERROR")
    sc.setSystemProperty("hive.metastore.uris", "")

    ssc = StreamingContext(sc,10)
    print("start reading data from kafka...")
    kvs = KafkaUtils.createDirectStream(ssc,[fpa_conf.topic], {"metadata.broker.list": fpa_conf.brokers})
    parsed = kvs.map(lambda v: json.loads(v[1])).flatMap(lambda post: post.values())

    if parsed is not None: 
        print("start analysising...")
        hc = getHiveContextInstance(sc)
        hc.sql("drop table if exists t_posts")
        posts = parsed.map(lambda r: (r['id'],r['message'],r['created_time'],\
            r['likes'],r['comment_count'],r['like_names']))
    
        posts.foreachRDD(createTable)   
        print('end analysising...')
        
    else:
        print('no data') 
    if ssc is not None:
        ssc.start()  
        ssc.awaitTermination() 
