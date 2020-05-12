ALTER TABLE categories RENAME "Topic" to topic;
ALTER TABLE categories RENAME "Description" to description;
ALTER TABLE categories RENAME "Title" to title;
ALTER TABLE categories RENAME catid to id;
ALTER TABLE categories RENAME fatherid to category_id;
ALTER TABLE categories DROP COLUMN lastupdate;
ALTER TABLE categories DROP COLUMN letterbar;
ALTER TABLE categories ADD COLUMN created_at timestamp without time zone DEFAULT NOW();
ALTER TABLE categories ADD COLUMN updated_at timestamp without time zone DEFAULT NOW();
ALTER TABLE categories ALTER id DROP DEFAULT;
DROP SEQUENCE categories_catid_seq;

ALTER TABLE externalpages ADD COLUMN domain_name varchar(255);
UPDATE externalpages SET domain_name = substring(link FROM '^(?:https?:\/\/)?(?:www\.)?([^\/]+)\/$');

ALTER TABLE externalpages RENAME catid TO category_id;
ALTER TABLE externalpages DROP COLUMN link;
ALTER TABLE externalpages DROP COLUMN "Title";
ALTER TABLE externalpages DROP COLUMN "Description";
ALTER TABLE externalpages ADD COLUMN created_at timestamp without time zone DEFAULT NOW();
ALTER TABLE externalpages ADD COLUMN updated_at timestamp without time zone DEFAULT NOW();

ALTER TABLE externalpages RENAME TO headlines_domains_categories;
ALTER TABLE categories RENAME TO headlines_categories;
