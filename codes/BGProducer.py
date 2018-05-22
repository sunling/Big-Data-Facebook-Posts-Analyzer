import facebook
import requests
from kafka import KafkaProducer, KafkaClient 
import json
import fpa_conf

producer = KafkaProducer(bootstrap_servers=[fpa_conf.brokers],api_version=(0,10))
graph = facebook.GraphAPI(fpa_conf.token)

def countLiksForPost(post): 
    like_names=[]
    likes = post['likes']
    for l in likes['data']:
        like_names.append(l['name'])
    while True:
        try:
            if 'paging' in likes:
                print("next page likes")
                likes = requests.get(likes['paging']['next']).json()
            for l in likes['data']:
                like_names.append(l['name'])
        except:
            break
    #print(like_names)
    return like_names

def parsePosts(posts):
    posts_dict = {}
    for post in posts: 
        postId =post['id']
        message = ""
        if 'message' in post:
            message = post['message']
        if 'story' in post:
            if message=="":
                message = post['story']
        like_names=[]
        if 'likes' in post:
             like_names = countLiksForPost(post)
        comment_count = countComments(post)
        
        posts_dict[postId] = {'id':postId,'message':message,\
            'created_time':post['created_time'],\
            'comment_count':comment_count,\
            'likes':len(like_names),'like_names':like_names}
        
    return posts_dict

def countComments(post):
    comment_count=0
    if 'comments' in post:
            if 'data' in post['comments']: 
                comments = post['comments'] 
                for cc in comments['data'] :
                    if cc['comment_count']==0 :
                        comment_count+=1
                    comment_count = comment_count + cc['comment_count']
                    while True:
                        try:
                            #print("next page comments")
                            comments = requests.get(comments['paging']['next']).json()
                            for cc in comments['data'] :
                                if cc['comment_count']==0 :
                                    cc['comment_count'] = 1
                                comment_count = comment_count + cc['comment_count']
                        except:
                            break
    return comment_count

def sendToKafka(data):
    msg = json.dumps(parsePosts(data))
    producer.send(fpa_conf.topic,msg.encode('utf-8'))

if __name__ == "__main__":
    posts = graph.get_object(fpa_conf.user+"/posts?fields=likes,message,created_time,comments{comment_count}") 
    sendToKafka(posts['data'])
    while True:
        try:
            print("next page")
            posts = requests.get(posts['paging']['next']).json()
            if 'data' in posts:
                data2 = posts['data']
                sendToKafka(data2)
        except KeyError:
            break
    print("done sending...")