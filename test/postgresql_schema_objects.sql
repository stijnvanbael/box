create type post_type as
(
    user_id   varchar,
    timestamp timestamp,
    text      varchar,
    keywords  character varying[]
);

create table "user"
(
    id        varchar primary key,
    name      varchar,
    last_post post_type,
    posts     post_type[]
);

create table "post"
(
    user_id   varchar,
    timestamp timestamp,
    text      varchar,
    keywords  varchar[],
    primary key(user_id, timestamp)
);
