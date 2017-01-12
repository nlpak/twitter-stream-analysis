import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object AnalyzeTwitters
{
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	
	def main(args: Array[String]) 
	{
	  val inputFile = args(0)  // uncomment to get command line file name
	  // val inputFile = "/home/najeeb/spark/wksp/DataExpl/Exercise3/part2.txt"
	  val conf = new SparkConf().setMaster("local[*]").setAppName("AnalyzeTwitters3")
	  val sc = new SparkContext(conf)
		
		
    // Comment these two lines if you want more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here
		
	println("Application will Generate Output file in current directory--------- Analyzetwitter.txt- FILE----------") 
	// reading the file 
	val file = sc.textFile(inputFile)
//    removing header 
	val header = file.first()
    val data = file.filter(row => row != header) 
//		                                            id      lang     lcode  langCount    max    min    re-count   text
    val rawRDD =data.map(_.split(",")).map(arr=>(arr(4),( arr(1) , arr(2), arr(3),     arr(5), arr(6), arr(7) , arr(8) ) )) 

// for understing writting tuple position of each elment in the rwa RDD
//		  0      1    2   3-lng    4                 5-max  6-min  7-count  8-txtm 9-lang-coded
//		((62),English,en,615,780367088808562688,642,   389,    254,    RT, @Joe_Sugg:, One 
//		
	                                                // max         min  
    val keyVaueRDD = rawRDD.map(element => (element._1,(element._2._4.toInt, element._2._5.toInt, element._2._7, element._2._2 ) )) // to get min max
    
    // minMaxRDD is like (key=IDofTweet, (Max, Min, text, language-code ) )
//    res57: (String, (Int, Int, String, String)) = (780267474323464193,(178,178,RT @iimz77: اللهم أجعلنا من الذين شفعت لهم ، وغفرت لهم ، وَ كتبت لهم الجنة .,(96)))
	  
    

    val minMaxRDD =  keyVaueRDD.reduceByKey((a, b) =>( Math.max(a._1.toInt, b._1.toInt),  // Max From Max column
                                                       Math.min(a._2.toInt, b._2.toInt), // Min from Mincolumn 
                                                       a._3,   // text of tweet
                                                       a._4 )) //language code
		
	  // res56: (String, String) = (780367088808562688,English)

    val langGroupRDD = rawRDD.map{ element => (element._1, element._2._1 ) }
		
		val joinLangMinMaxRDD = langGroupRDD.join(minMaxRDD)
//  = (780267474323464193-id,(Persian-languge,(178-max,17-min)))   
		                                   //   lang      (id, max, min, total count, language-code)
		val resultV1 = joinLangMinMaxRDD.map{ rdd => (rdd._2._1,      // language 
		                                             (rdd._1,         // IDoFTweet 
		                                              rdd._2._2._1,   // max 
		                                              rdd._2._2._2,   // min       
		                                              rdd._2._2._1 - rdd._2._2._2 + 1,  // count of total tweets
		                                              rdd._2._2._3,     //text of the tweet
		                                              rdd._2._2._4 ))   // language code
		                                     }

		
	  val groupByLanguage = resultV1.groupBy(_._1)
	
	  // GroupBy Language like (langage, totatlTweetofThatLanguage)
	  val groupByLanguageSum = groupByLanguage.mapValues(_.map(_._2._4.toInt).sum) 
 
	  val resultV2 = resultV1.join(groupByLanguageSum) 
//	 output will be like  (Ukrainian,((780367478715064320,1,1,1),143))
	  
	  
	  val resultWithTotalRetweetCount = resultV2.map{ rdd => ((rdd._1, rdd._2._1._6, rdd._2._2.toInt.toLong), // (languge, language code, retweetCount in that language) as key
                                                  ( rdd._2._1._3 - rdd._2._1._4 + 1, // total tweets of that tweetID
                                                    rdd._2._1._1,   // id of the Tweet
                                                    rdd._2._1._3,   // min retweets 
                                                    rdd._2._1._4,  // max retweets
                                                    rdd._2._1._5 ))   // text of tweet 
                                                  }
	  
//     output will be like  (Ukrainian,uk,143),(1,780367478715064320,RT @Ruslanmdes: Про поліцію,1,1))  
      	
	    
	   val filterUnitRetweetCount = resultWithTotalRetweetCount.filter(element => element._2._1 > 1)

	   val uniquKeyValRDD =  filterUnitRetweetCount.map{ rdd=> (rdd._2._2,   // ID of the Tweet
	                                       (rdd._1._3,   // TweetCountofThatLanguage 
	                                        rdd._1._1,   // language 
                                          rdd._1._2,   // Lanagueg code 
                                          rdd._2._1,   // RetweetCount
                                          rdd._2._5 ) ) // Text of tweet
	                                   } 
      
      // Reducing elments based on tweeter ID, I was not sure this requirements, all records in the file got aggrigated no will write output one entry for each tweeetID 
	  val uniqueElementRDD = uniquKeyValRDD.reduceByKey{ (a, b ) => ( a._1, a._2, a._3, a._4, a._5 ) } 
      val outputRDD = uniqueElementRDD.map( element => (element._2._2, element._2._3, element._2._1, element._1, element._2._4, element._2._5))
    

        // First sort on the TotalRetweetCount of that tweet 
      val sortByRetweetCount = outputRDD.sortBy(_._5, false)  
      
        // 2nd it will resort as per total language count, but in same language, tweets having more count will apear first
      val sortBylang =  sortByRetweetCount.sortBy(_._3, false)
      
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("Analyzetwitter.txt"), "UTF-8"))
	  bw.write("Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")  
	  // writting output file   
      sortBylang.collect().foreach{
            x => bw.write(x.toString()+ "\n")   
      }
	  	
      bw.close
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

