drop table if exists votetypes;

create external table votetypes (
  id  string,
  vote_type string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," ESCAPED BY '\\'
STORED AS TEXTFILE location '/user/vagrant/hive/stackoverflow/votetypes';

drop table if exists posttypes;

create external table posttypes (
  id  string,
  post_type string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," ESCAPED BY '\\'
STORED AS TEXTFILE location '/user/vagrant/hive/stackoverflow/posttypes';

drop table if exists linktypes;

create external table linktypes (
  id  string,
  link_type string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," ESCAPED BY '\\'
STORED AS TEXTFILE location '/user/vagrant/hive/stackoverflow/linktypes';


drop table if exists users;
  
CREATE EXTERNAL TABLE users(
  id string,
  reputation string,
  creationdate string,
  displayname string,
  lastaccessdate string,
  websiteurl string,
  location string,
  aboutme string,
  views string,
  upvotes string,
  downvotes string,
  age string,
  accountid string,
  profileimageurl string
  )
STORED AS AVRO location '/user/vagrant/hive/stackoverflow/users';

drop table if exists badges;

create external table badges(
  id     string,
  userid string,
  name   string,
  date   string
)
stored as avro location '/user/vagrant/hive/stackoverflow/badges';


drop table if exists posts;

create external table posts(
  id            string,
  posttypeid    string,
  acceptedanswerid   string,
  parentid   string,
  creationdate string,
  score        string,
  viewcount    string,
  body         string,
  owneruserid  string,
  ownerdisplayname string,
  lasteditoruserid  string,
  lasteditordisplayname string,
  lasteditdate     string,
  lastactivitydate string,
  title            string,
  tags             string,
  answercount      string,
  commentcount     string,
  favoritecount    string,
  closeddate       string,
  communityowneddate string
)
stored as avro location '/user/vagrant/hive/stackoverflow/posts';

drop table if exists comments;

create external table comments(
  id            string,
  postid        string,
  score         string,
  text          string,
  creationdate  string,
  userdisplayname string,
  userid          string
)
stored as avro location '/user/vagrant/hive/stackoverflow/comments';


drop table if exists tags;

create external table tags(
  id            string,
  tagname       string,
  count         string,
  excerptpostid  string,
  wikipostid    string
)
stored as avro location '/user/vagrant/hive/stackoverflow/tags';


drop table if exists votes;

create external table votes(
  id            string,
  postid        string,
  votetypeid    string,
  userid        string,
  creationdate  string,
  bountyamount  string
)
stored as avro location '/user/vagrant/hive/stackoverflow/votes';


drop table if exists postlinks;

create external table postlinks(
  id            string,
  creationdate  string,
  postid        string,
  relatedpostid string,
  linktypeid    string
)
stored as avro location '/user/vagrant/hive/stackoverflow/postlinks';
