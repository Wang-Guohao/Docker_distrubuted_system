CREATE DATABASE datacenter;
use datacenter;

CREATE TABLE `Movie` (
  `id` varchar(12) NOT NULL PRIMARY KEY,
  `title` varchar(64) NOT NULL DEFAULT '',
  `year` int NOT NULL,
  `image` varchar(256) NOT NULL DEFAULT '',
  `releaseState` varchar(64) NOT NULL DEFAULT '',
  `runtimeMins` int NOT NULL DEFAULT '0',
  `plot` varchar(500),
  `contentRating` varchar(12)
) ENGINE=InnoDB;


CREATE TABLE `user` (
  `id` int NOT NULL AUTO_INCREMENT,
  `username` varchar(35) NOT NULL DEFAULT '',
  `password` varchar(35) NOT NULL DEFAULT '',
  `status` int NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1;

INSERT INTO `user`
  (id, username, password, status)
VALUES
  (1, 'user1', '1111', 0),
  (2, 'user2', '2222', 0),
  (3, 'user3', '3333', 0);

CREATE TABLE `genre` (
  `tid` varchar(12) NOT NULL,
  `genre` varchar(24) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

CREATE TABLE `director` (
  `tid` varchar(12) NOT NULL,
  `nid` varchar(35) NOT NULL DEFAULT '',
  `name` varchar(128) NOT NULL DEFAULT ''
) ENGINE=InnoDB;

CREATE TABLE `star` (
  `tid` varchar(12) NOT NULL,
  `nid` varchar(35) NOT NULL DEFAULT '',
  `name` varchar(128) NOT NULL DEFAULT ''
) ENGINE=InnoDB;



CREATE TABLE `topic` (
  `topicid` int NOT NULL AUTO_INCREMENT,
  `topic` varchar(35) NOT NULL DEFAULT '',
  `advertise` int NOT NULL DEFAULT 0,
  PRIMARY key (`topicid`)
) ENGINE=InnoDB;


INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('Horror', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('Action', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('Drama', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('Other', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('PG-13', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('R', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('18', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('<60 min', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('60 to 120 min', 0);
INSERT INTO `datacenter`.`topic`(`topic`,`advertise`)VALUES('>120 min', 0);

CREATE TABLE `subscription`(
  `subid` int not null AUTO_INCREMENT,
  `userid` int not null,
  `topicid` int not null,
  `name` varchar(24) NOT NULL DEFAULT '',
  PRIMARY key (`subid`)
) ENGINE=InnoDB;


CREATE TABLE `event`(
  `eventid` int not null AUTO_INCREMENT,
  `content` varchar(2000) NOT NULL DEFAULT '',
  `topicid` int NOT NULL DEFAULT 4,
  PRIMARY key (`eventid`)
) ENGINE=InnoDB;


CREATE TABLE `user_event` (
  `ueid` int NOT NULL AUTO_INCREMENT,
  `userid` varchar(35) NOT NULL DEFAULT '',
  `eventid` varchar(128) NOT NULL DEFAULT '',
  `unread` int DEFAULT 1,
  `createdtime` timestamp not NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY key (`ueid`)
) ENGINE=InnoDB;


INSERT INTO `datacenter`.`subscription` (`userid`,`topicid`)VALUES(1,1);
INSERT INTO `datacenter`.`subscription` (`userid`,`topicid`)VALUES(2,2);
INSERT INTO `datacenter`.`subscription` (`userid`,`topicid`)VALUES(1,2);
INSERT INTO `datacenter`.`subscription` (`userid`,`topicid`)VALUES(1,3);