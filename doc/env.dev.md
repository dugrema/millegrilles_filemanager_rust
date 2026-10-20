# Application Documents

## Exposer les ports du middleware

Utiliser le script sous le projet millegrilles.instance.python, repertoire `bin/dev/publish_ports.sh` pour exposer
les ports de redis (6379), MQ (5673) et MongoDB (27017).

## Paramètres

<pre>
BACKUP_PATH=/home/mathieu/tas/dev/millegrilles/dev1/var/backup/domains
CAFILE=/home/mathieu/tas/dev/millegrilles/dev1/etc/millegrille.pem
KEYFILE=/home/mathieu/tas/dev/millegrilles/dev1/secrets/filemanager.pem
MG_MONGO_HOST=localhost
MG_MQ_HOST=localhost
MG_REDIS_PASSWORD_FILE=/home/mathieu/tas/dev/millegrilles/dev1/secrets/redis.txt
MG_REDIS_URL=rediss://client_rust@localhost:6379#insecure
RUST_LOG=warn,millegrilles_filemanager_rust=debug,millegrilles_common_rust=debug
DEV=1
</pre>

## Command line parameters

run --package millegrilles_filemanager_rust --bin millegrilles_filemanager_rust -- --restore --capath /path/to/ca.pem --noresume
