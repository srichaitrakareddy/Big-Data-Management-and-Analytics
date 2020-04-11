import pymongo
from pymongo import MongoClient
from newsplease import NewsPlease

def dump(obj):
  for attr in dir(obj):
    print("obj.%s = %r" % (attr, getattr(obj, attr)))

def insert_data_into_db(article):
	retrieved_from_newsplease = {
									"authors": article.authors,
									"date_download": article.date_download,
									"date_modify" : article.date_modify,
									"date_publish" : article.date_publish,
									"description" : article.description,
									"filename" : article.filename,
									"image_url" : article.image_url,
					                "language" : article.language,
					                "localpath" : article.localpath,
					                "title" : article.title,
					                "title_page" : article.title_page,
					                "title_rss" : article.title_rss,
					                "source_domain" : article.source_domain,
					                "text" : article.text,
					                "url" : article.url
								}

	#Now we insert this document to the database collection (Table).
	insert_status = myTable.insert_one(retrieved_from_newsplease)
	print("Insertion status is ",insert_status)
	print("Article inserted is ",insert_status.inserted_id)


def retrieve_records():
	print("Lets now check the number of rows inserted with rows")
	rows = myTable.find()
	count = 0;
	for row in rows:
		print("--------------------ROW %s------------------"%(count))
		print("Authors are: ", row["authors"]);
		print("date_download are: ", row["date_download"]);
		print("date_modify are: ", row["date_modify"]);
		print("date_publish are: ", row["date_publish"]);
		print("description are: ", row['description']);
		print("filename are: ", row['filename']);
		print("image_url are: ", row['image_url']);
		print("language are: ", row['language']);
		print("localpath are: ", row['localpath']);
		print("title are: ", row['title']);
		print("title_page are: ", row['title_page']);
		print("source_domain is: ", row['source_domain']);
		print("text are: ", row['text']);
		print("url are: ", row['url']);
		print("--------------------Till Here-------------------------------")
		count+=1

#Main starts here.

#Change the connection to yours.
client = MongoClient('mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb', 27017)
db = client.spanish_news #We use our database now.
print(db)
myTable = db.crawled_data
urls = ['https://www.am.com.mx/guanajuato',
		#'https://www.am.com.mx/lapiedad'
		'https://www.am.com.mx/leon',
		'https://www.am.com.mx/sanfranciscodelrincon',
		'https://www.mural.com/',
		'https://www.eldiariodechihuahua.mx/Delicias/',
		'https://www.elsoldeparral.com.mx/',
		'https://www.elnorte.com/',
		'http://www.el-mexicano.com.mx/inicio.htm',
		'https://www.elsudcaliforniano.com.mx/',
		'https://www.diariodequeretaro.com.mx/',
		'https://www.eloccidental.com.mx/',
		'https://www.elsoldemexico.com.mx/',
		'https://www.lavozdelafrontera.com.mx/',
		'https://www.elsoldesanluis.com.mx/',
		'http://www.milenio.com/temas/torreon',
		'http://www.milenio.com/estado-de-mexico',
		'http://www.milenio.com/leon',
		'http://www.milenio.com/hidalgo',
		'http://www.milenio.com/jalisco',
		'http://www.milenio.com/monterrey',
		'http://www.milenio.com/puebla',
		'http://www.milenio.com/tamaulipas',
		'http://www.milenio.com/temas/xalapa'
	]

articles = NewsPlease.from_urls(urls, timeout=3)
#print(len(articles))
for url in urls:
	#dump(articles[url])
	insert_data_into_db(articles[url])
	retrieve_records()
