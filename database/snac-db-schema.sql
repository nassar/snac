create table blog (
    blog_id bigserial not null,
    primary key (blog_id),
    link text not null,
    unique (link)
);    

create table blog_rank_run (
    blog_rank_run_id bigserial not null,
    primary key (blog_rank_run_id),
    start_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    end_time timestamp with time zone
);

create table blog_rank_log (
    log_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    blog_rank_run_id bigint not null,
    foreign key (blog_rank_run_id) references blog_rank_run (blog_rank_run_id),
    log_level text not null,
    message text not null
);

create table blog_rank (
    blog_rank_run_id bigint not null,
    foreign key (blog_rank_run_id) references blog_rank_run (blog_rank_run_id),
    blog_id bigint not null,
    foreign key (blog_id) references blog (blog_id),
    primary key (blog_rank_run_id, blog_id),
    rank integer not null,
    auth_score integer not null
);

create view blog_rank_latest as (
select br.blog_rank_run_id, br.blog_id, b.link, br.rank, br.auth_score
    from blog_rank br
        join blog b on
	    b.blog_id = br.blog_id
    where blog_rank_run_id in (
        select distinct blog_rank_run_id
            from blog_rank_run
	    where end_time is not null
	    order by blog_rank_run_id desc
            limit 1
    )
);

create table blog_post_run (
    blog_post_run_id bigserial not null,
    primary key (blog_post_run_id),
    start_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    end_time timestamp with time zone
);

create table blog_post_log (
    log_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    blog_post_run_id bigint not null,
    foreign key (blog_post_run_id) references blog_post_run (blog_post_run_id),
    log_level text not null,
    message text not null
);

create table blog_post (
    blog_post_id bigserial not null,
    primary key (blog_post_id),
    blog_post_run_id bigint not null,
    foreign key (blog_post_run_id) references blog_post_run (blog_post_run_id),
    blog_id bigint not null,
    foreign key (blog_id) references blog (blog_id),
    author text not null,
    content text not null,
    link text,
    unique (link),
    published text,
    title text not null
);

create table blog_post_link (
    blog_post_id bigint not null,
    foreign key (blog_post_id) references blog_post (blog_post_id),
    link text not null,
    primary key (blog_post_id, link)
);

create table blog_post_blog_link (
    blog_post_id bigint not null,
    foreign key (blog_post_id) references blog_post (blog_post_id),
    link text not null,
    primary key (blog_post_id, link),
    blog_id bigint not null,
    foreign key (blog_id) references blog (blog_id)
);

create view blog_post_blog_link_view as (
select b.blog_id, b.link blog_link, bp.blog_post_id, bp.link blog_post_link,
       bpbl.link cited_link, bpbl.blog_id cited_blog_id
    from blog_post_blog_link bpbl
        join blog_post bp on
	    bpbl.blog_post_id = bp.blog_post_id
	join blog b on
	    bp.blog_id = b.blog_id
    where b.blog_id <> bpbl.blog_id
    order by blog_id, blog_post_id, cited_blog_id
);

create table blog_roll (
    blog_roll_id bigserial not null,
    primary key (blog_roll_id),
    blog_post_run_id bigint not null,
    foreign key (blog_post_run_id) references blog_post_run (blog_post_run_id),
    blog_id bigint not null,
    foreign key (blog_id) references blog (blog_id),
    link text not null,
    unique (blog_id, link),
    title text
);

create table blog_roll_blog_link (
    blog_roll_id bigint not null,
    foreign key (blog_roll_id) references blog_roll (blog_roll_id),
    blog_id bigint not null,
    foreign key (blog_id) references blog (blog_id),
    primary key (blog_roll_id, blog_id)
);

-- grant select on all tables in schema public to sarcomere;

