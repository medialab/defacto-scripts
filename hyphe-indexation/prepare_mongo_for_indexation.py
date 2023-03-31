import os
import sys
import casanova
import pymongo
import tqdm
from minet.cli.loading_bar import LoadingBar


def read_config(path="config.inc"):
    conf = {}
    try:
        with open(path) as f:
            for line in f.read().split("\n"):
                if line and "=" in line:
                    key, value = line.split("=", 1)
                    conf[key] = value.strip('"')
    except Exception as e:
        sys.exit("can't read config.inc: %s - %s" % (type(e), e))
    print("CONF:", conf, file=sys.stderr)
    return conf


def connect_mongo(conf):
    print("connecting to mongo...")
    try:
        mongo = pymongo.MongoClient(conf["MONGO_HOST"], int(conf["MONGO_PORT"]))
        corpus = "hyphe_%s" % conf["HYPHE_CORPUS"]
        db = mongo[corpus]
        print("%s pages in MongoDB" % db["pages"].count_documents({}))
    except Exception as e:
        sys.exit("can't connect to mongo: %s - %s" % (type(e), e))

    print("building indexes...")
    try:
        db["pages"].create_index([('indexed', pymongo.ASCENDING)])
        db["pages"].create_index([('timestamp', pymongo.ASCENDING)])
        db["pages"].create_index([('indexed', pymongo.ASCENDING), ('timestamp', pymongo.ASCENDING)])
        db["pages"].create_index([('text_indexation_status', pymongo.ASCENDING)])
        db["pages"].create_index([('text_indexation_status', pymongo.ASCENDING), ('forgotten', pymongo.ASCENDING)])
    except Exception as e:
        sys.exit("can't create mongo indexes: %s - %s" % (type(e), e))

    return db


def process_pages(pages_dir, db):

    todo = db["pages"].count_documents({
        'text_indexation_status': {'$nin': ['TO_INDEX', 'DONT_INDEX']}
    })
    if not todo:
        sys.exit("ALL PAGES ALREADY PREPARED")
    print(todo)

    db["pages"].update_many(
        {'text_indexation_status': {
            '$nin': ['TO_INDEX', 'DONT_INDEX']
        }},
        {'$set': {
            'text_indexation_status': 'DONT_INDEX'
        }},
        upsert=False
    )
    db["jobs"].update_many({}, {'$unset': {'text_indexed': True}})

    pages_file = os.path.join(pages_dir, "pages.csv")
    total = casanova.count(pages_file)
    with LoadingBar(title='Preparing pages in MongoDB', unit='pages', total=total) as loading_bar, open(pages_file) as f:
        reader = casanova.reader(f)
        url_pos = reader.headers['url']
        webentity_pos = reader.headers['webentity']
        content_pos = reader.headers['content_type']
        status_pos = reader.headers['status']

        for page in reader:
            if page[content_pos] in ["text/plain", "text/html"] and page[status_pos] == "200":
                try:
                    db.pages.update_one(
                        {'url': page[url_pos]},
                        {'$set': {
                            'text_indexation_status': 'TO_INDEX',
                            'webentity_when_crawled': page[webentity_pos]
                        }}
                    )
                except Exception as e:
                    print("Error while updating page %s: %s - %s" % (page[url_pos], type(e), e), file=sys.stderr)

            loading_bar.advance()


if __name__ == "__main__":
    conf = read_config()
    db = connect_mongo(conf)
    process_pages(conf["PAGES_DIR"], db)

