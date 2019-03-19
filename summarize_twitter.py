import glob
import cld2 #TODO: Need to finish this and include in output.
import gzip
import json
import sys
import re
import multiprocessing
import datetime
import logging
import os

#TODO: Anyting else to include in summary file?
#see https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json
OUTPUT_STR="|".join(["{created_at}","{id}","{tweet_lang}",
	"{user_id}","{user_created_at}", "{user_lang}", "{location}","{coordinates}", "{statuses_count}",
	"{time_zone}", "{utc_offset}","{retweet}", #"{followers_count}", "{friends_count}",
	"{cld_reliable}", "{cld_bytes}", "{cld_lang1}", "{cld_lang1_percent}", "{cld_lang2}", "{cld_lang2_percent}",
	"{text}"
	])+"\n"

#TODO: Decide final format.
# * Use "|" to separate fields.
# * Should text fields be enclosed in quotation marks?
# * Or, should "|" be replaced / escaped in text?

RE_CR_LF=re.compile(r"[\r\n]")
RE_MENTION=re.compile(r"@[a-zA-Z0-9_]+")
RE_URL=re.compile(r"https?://\S+")

#This method configures a different logger for each process
#so each process writes to their own file and then the different
#logs are compiled into a single file later
def start_logging():
	logfile = open("Log_File_{0}.log".format(str(os.getpid())), 'w')
	logging.basicConfig(filename = "Log_File_%s.log" % os.getpid(), level =logging.DEBUG)

#This takes all the seperate log files generated and compiles
#them to a single file
def compile_logs():
	#TODO: better log file name since this only creates one logfile a day which isnt too useful
	logfiles = glob.glob('*.log')
	finalLog = open("Log_For_%s.log" % str(datetime.datetime.utcnow())[:11], 'w')
	for file in logfiles:
		with open(file, 'r') as f:
			finalLog.write(f.read())
		os.remove(file)

def summarize_file(infile):
	start_logging()
	out_files = {}
	#currently we just have ExampleData as the directory since path is specified later
	outDirBase = "./ExampleData/"
	with gzip.open(infile,"rt") as fh:
		for line in fh:
			vals=summarize_tweet(line)
			if vals!=None:
				filename = vals["created_at"][:4] + "-" + vals["created_at"][4:6] + "-" + vals["created_at"][6:8] + "_" + vals["created_at"][8:10] + "00_WW_public.twt.gz"
				file_path = outDirBase + filename
				if not os.path.isfile(file_path):
					out_files[str(filename)] = gzip.open(file_path,"wt",encoding='utf-8') #create new file
				elif not str(filename) in out_files:
					out_files[str(filename)] = gzip.open(file_path,"at",encoding='utf-8') #TODO should it append to existing file or replace the existing; currently append to existing file
				out_files[str(filename)].write(OUTPUT_STR.format(**vals))
		fh.close()
	for key in out_files:
		try:
			out_files[key].close()
		except Exception as e:
			logging.debug("Failed to close twt file: "+ str(e) +"\n"+key)

def summarize_tweet(rawtweet):
	if rawtweet==None or rawtweet.strip()=="":
		return None

	try:
		tweet=json.loads(rawtweet)
	except Exception as e:
		logging.debug("Failed to parse JSON: "+ str(e) +"\n"+rawtweet)
		return None

	if not "id" in tweet:
		return None

	vals={}

	try:
		vals["id"]=tweet["id_str"]
		#Format tiemstamps to match TV format e.g., 20181211000034.967 (YYYYmmddHHMMSS.000)
		#Tweets to do not have units more precise than the second (omit .000)
		created_at=tweet["created_at"].replace(" +0000 "," ")
		created_at=datetime.datetime.strptime(created_at,"%a %b %d %H:%M:%S %Y")
		created_at=created_at.strftime("%Y%m%d%H%M%S")
		vals["created_at"]=created_at
		vals["tweet_lang"]=tweet["lang"] if "lang" in tweet else "NA"


		user=tweet["user"]
		vals["user_id"]=user["id_str"]

		user_created_at=user["created_at"].replace(" +0000 "," ")
		user_created_at=datetime.datetime.strptime(user_created_at,"%a %b %d %H:%M:%S %Y")
		user_created_at=user_created_at.strftime("%Y%m%d%H%M%S")
		vals["user_created_at"]=user_created_at
		vals["followers_count"]=user["followers_count"]
		vals["friends_count"]=user["friends_count"]
		vals["user_lang"]=user["lang"]

		if user["location"]!=None:
			#TODO: How to handel | in text?
			vals["location"]=RE_CR_LF.sub(" ",user["location"]).replace("|","\\|")
		else:
			vals["location"]="NA"

		#Added field for tweet coordinates - Aaron Weinberg
		if tweet["coordinates"]!=None:
			#[Long,Lat] is how it will be saved as this is how twitter gives us the information
			vals["coordinates"] = tweet["coordinates"]["coordinates"]
		elif tweet["place"]!=None:
			if tweet["place"]["bounding_box"]!=None:
				vals["coordinates"] = tweet["place"]["bounding_box"]["coordinates"][0] #TODO change to bounding box of GPS Coords.
			else:
				vals["coordinates"]="NA"
		else:
			vals["coordinates"]="NA"

		#add retweet field, id of the original tweet or NA - Aaron Weinberg
		vals["retweet"]=tweet["retweeted_status"]["id_str"] if "retweeted_status" in tweet else "NA"

		vals["statuses_count"]=user["statuses_count"]
		vals["time_zone"]=user["time_zone"]
		vals["utc_offset"]=user["utc_offset"]


		if "extended_tweet" in tweet and "full_text" in tweet["extended_tweet"]:
			txt=tweet["extended_tweet"]["full_text"]
		else:
			txt=tweet["text"]

		#TODO: implement link checking -Taylor

		#TODO: How to handel | in text?
		vals["text"]=RE_CR_LF.sub(" ",txt).replace("|","\\|")

		#here is where we must implement CLD2 from the method below -Taylor
		#TODO: figure out if this is done or requires more code/ test it
		langVals=detect_tweet_lang(vals["text"])
		for key in langVals:
			vals[key]=langVals[key]

		return vals
	except Exception as e:
		logging.debug("Failed to extract attributes: "+ str(e) +"\n"+rawtweet)
	return None


#TODO: Work on incorporating this data to our .twt
def detect_tweet_lang(text):
	try:
		#Remove mentions and URLs before trying to detect language
		text=RE_MENTION.sub(" ",text)
		text=RE_URL.sub(" ",text)
		vals={}

		text = text.encode("UTF-8")
		vals["cld_reliable"], vals["cld_bytes"], details = cld2.detect(text)
		if len(details)>1:
			vals["cld_lang1"]=details[0][1]
			vals["cld_lang1_percent"]=details[0][2]
		if len(details)>2:
			vals["cld_lang2"]=details[1][1]
			vals["cld_lang2_percent"]=details[1][2]
		return vals
	except Exception as e2:
		logging.debug("CLD error: "+ str(e2) +"\n"+text)



if __name__=="__main__":
	files=glob.glob("./ExampleData/*.json.gz") #For testing locally - Aaron Weinberg
	#files=glob.glob("../tmp/*.json.gz")
	#TODO: Multiprocess on final pass
	#with multiprocessing.Pool(1) as pool:
	#	pool.imap_unordered(summarize_file,files)
	for file in files:
		summarize_file(file)
		#TODO: Move original and summary file to proper location in directory tree and rename, e.g., 2018-12-11_0000_WW_Twitter_Spritzer.twt
		#TODO: Is everyone happy with "WW" for "world-wide" / non-geographic data?
	compile_logs()
