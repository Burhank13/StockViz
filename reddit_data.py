import pandas as pd
import dateutil.relativedelta
from datetime import date
import datetime
import yfinance as yf
import numpy as np
import praw
import sqlite3


# return a dataframe for the newest reddit posts
def get_reddit(cid='Ic8y6tMJHDJ9yQ', csec='iT1kAugDydlph6jBgjT6dApq9T8Jwg', uag='StockViz', subreddit='wallstreetbets'):
    reddit = praw.Reddit(client_id=cid, client_secret=csec, user_agent=uag)

    # print(reddit.read_only)

    posts = reddit.subreddit(subreddit).new(limit=None)
    #hot_bets = reddit.subreddit('wallstreetbets').hot(limit=1000)
    p = []
    for post in posts:
        p.append([post.title, post.score, post.selftext])
    posts_df = pd.DataFrame(p, columns=['title', 'score', 'post'])
    return posts_df
