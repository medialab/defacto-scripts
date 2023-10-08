
# NOTE : optimisation probable en exploitant les IDs natifs ObjectID mongo plutôt que les timestamps

# On récupère tous les timestamps des pages dans un csv
k exec -n hyphe-medias db-6459f47478-9hjtx -- mongoexport -d hyphe_m-dias-fr-07-23 --type=csv -c pages -f timestamp -o /data/db/mongodump/pages-timestamps.csv

# On le rapatrie
scp boo@k8s-nfs-prod-01.medialab.sciences-po.fr:/srv/nfs/pvc-4a1254e9-9812-4dfd-9d29-6ad12eaafa97/mongodump/pages-timestamps.csv hyphe-IN-pages/

# On le sort puis on regarde les premières et dernières lignes pour établir la gamme de timestamps
xsv sort hyphe-IN-pages/pages-timestamps.csv > hyphe-IN-pages/pages-timestamps.sort
head -2 hyphe-IN-pages/pages-timestamps.csv && tail -1 hyphe-IN-pages/pages-timestamps.csv

# On compte le nombre de lignes puis on splitte le fichier en 16 (l'export crashe en ram à 7%)
total=$(xsv count hyphe-IN-pages/pages-timestamps.sort)
xsv split -s $((total / 16 + 1)) --filename pages-timestamps-{}.csv hyphe-IN-pages hyphe-IN-pages/pages-timestamps.sort

# On lance un mongodump des pages pour chaque segment de timestamps
ls hyphe-IN-pages/pages-timestamps-* | while read cs; do
  gte=$(head -2 $cs | tail -1)
  lte=$(tail -1 $cs)
  echo $cs $gte $lte
  k exec -n hyphe-medias db-6459f47478-9hjtx -- mongodump -d hyphe_m-dias-fr-07-23 -c pages --gzip -o /data/db/mongodump/$gte -q '{$and: [{timestamp: {$gte: '$gte'}}, {timestamp: {$lte: '$lte'}}]}'
done

# On dumpe aussi le reste des collections (utiles) une par une
for col in creationrules jobs webentities; do
  k exec -n hyphe-medias db-6459f47478-9hjtx -- mongodump -d hyphe_m-dias-fr-07-23 -c $col --gzip -o /data/db/mongodump/$col
done
k exec -n hyphe-medias db-6459f47478-9hjtx -- mongodump -d hyphe -c corpus --gzip -o /data/db/mongodump/corpus

# On rapatrie le tout
scp -r boo@k8s-nfs-prod-01.medialab.sciences-po.fr:/srv/nfs/pvc-4a1254e9-9812-4dfd-9d29-6ad12eaafa97/mongodump/ hyphe-IN-pages/                                                   


# On loade les dumps en local
ls hyphe-IN-pages | grep -v csv | while read dump; do
  mongorestore --gzip $dump
done


# TODO find a way to build pages.csv with page/webentity association without crashing container



# On prépare les données pour l'indexation
pyenv activate defacto
python prepare_mongo_for_indexation.py


# TODO index using the docker containers
cd ~/dev/hyphe-elastic/hyphe_text_indexation
pyenv activate hyphe-elastic
sudo service elasticsearch restart
python text_indexation.py


