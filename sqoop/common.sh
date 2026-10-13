# Shared by the Sqoop scripts: JDBC URL and an HDFS password file (Sqoop jobs must not store passwords).
CATALOGUE_JDBC="jdbc:postgresql://postgres:5432/catalogue"
PASSWORD_FILE="/user/root/.catalogue.password"
if ! hdfs dfs -test -e "$PASSWORD_FILE"; then
  printf '%s' "$CATALOGUE_DB_PASSWORD" | hdfs dfs -put - "$PASSWORD_FILE"
  hdfs dfs -chmod 400 "$PASSWORD_FILE"
fi
