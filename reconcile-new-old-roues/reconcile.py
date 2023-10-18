from sys import stderr
from csv import DictReader
from casanova import enricher
from ural.lru import LRUTrie


old_medias = {}
old_headers = []
trie = LRUTrie(strip_trailing_slash=True)
with open("medias_Fr_2020.csv") as f:
    for row in DictReader(f):
        if not old_headers:
            old_headers = ["old_twitter"] + ["old_%s" % k for k in row.keys()]

        twitters = set()
        for url in row["prefixes"].split("|"):
            trie.set(url, row["webentity_id"])
            if "twitter.com/" in url:
                tw = url.split("twitter.com/")[1].split("/")[0].lower().replace("%40", "")
                twitters.add(tw)

        row["twitter"] = "|".join(twitters)

        old_medias[row["webentity_id"]] = row


with open("medias_Fr_2023.csv") as f, \
     open("medias_Fr_2023+metas2020.csv", "w") as of:
    rows = enricher(f, of, add=old_headers)
    name_pos = rows.headers["NAME"]
    prefixes_pos = rows.headers["PREFIXES AS URL"]

    for row in rows:
        matches = []
        for url in row[prefixes_pos].split(" "):
            old = trie.match(url)
            if old and old not in matches:
                matches.append(old)

        olddata = [None for k in old_headers]
        if len(matches) == 1:
            olddata = [old_medias[matches[0]][k.replace("old_", "")] for k in old_headers]
        elif len(matches) > 1:
            print("WARNING: found multiple old WebEntities matching %s:" % row[name_pos], row[prefixes_pos], [old_medias[i]["name"] for i in matches], file=stderr)
        else:
            print("INFO: could not find %s in old WebEntities:" % row[name_pos], row[prefixes_pos], file=stderr)

        rows.writerow(row, olddata)

