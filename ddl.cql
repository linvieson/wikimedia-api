CREATE KEYSPACE wiki WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wiki;

CREATE TABLE IF NOT EXISTS pages_by_domain (uid text, domain text, rev_timestamp text, user_is_bot boolean, user_id text, PRIMARY KEY ((uid), domain));

CREATE TABLE IF NOT EXISTS pages_by_user (uid text, domain text, rev_timestamp text, user_is_bot boolean, user_id text, PRIMARY KEY ((uid), user_id));

CREATE TABLE IF NOT EXISTS pages_by_timestamp (uid text, page_title text, rev_timestamp text, user_id text, PRIMARY KEY ((uid), rev_timestamp));
