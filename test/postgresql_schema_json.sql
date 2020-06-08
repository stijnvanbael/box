create table "user"
(
    id        varchar primary key,
    name      varchar,
    last_post json,
    posts     json
);

create table "post"
(
    user_id   varchar,
    timestamp timestamp,
    text      varchar,
    keywords  json,
    primary key(user_id, timestamp)
);
