# twitter-stream-analysis

The goal of this program is to performe some twitter-stream-analysis on the data that we collected from streaming.

Read the output log of streaming (programe 2), and calculate the retweet counts for the tweets. For
counting the retweet counts, you have to search for the retweets in the whole log for
MinRetweetCounts and MaxRetweetCounts, to see the mimimum value of
MinRetweetCounts and the maximum value of MaxRetweetCounts, and calculate the
retweet count through them. This can be better explained by an example. Let us say, we
have a tweet X, whose minimum MinRetweetCounts value can be found at 75th second, and
that minimum value is 64, while its maximum MaxRetweetCounts values is found at 380th
second and that maximum value is 364, then the RetweetCount for that tweet is 364 – 64 +
1 = 301.

When you print the output, just like in Exercise 2, you have to group the tweets by
Language, and sort the tweets by the total retweet counts (combined sum of all the retweet
counts) in those languages. This means that tweets for the language with the most retweet
counts would appear first, and others would follow in descending order. For the tweets of
the same language, the one with the most retweet count would appear first. Likewise,
others would follow in descending order.

You should not show the tweets which have been retweeted less than 2 times. Basically, you
just have to filter them out. However, when calculating the value of
TotalRetweetsInThatLang, you must also consider retweets who have been tweeted only
once.

The format of the output should be the following,
Language,Languagecode,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text
