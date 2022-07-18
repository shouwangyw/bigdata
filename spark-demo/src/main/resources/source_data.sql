/*
SQLyog  v12.2.6 (64 bit)
MySQL - 5.7.27 : Database - mydb
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE IF NOT EXISTS `mydb` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `mydb`;

/*Table structure for table `scores` */

DROP TABLE IF EXISTS `scores`;

CREATE TABLE `scores` (
  `sno` varchar(3) NOT NULL,
  `cno` varchar(5) NOT NULL,
  `degree` decimal(10,1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `scores` */

insert  into `scores`(`sno`,`cno`,`degree`) values 
('103','3-245',86.0),
('105','3-245',75.0),
('109','3-245',68.0),
('103','3-105',92.0),
('105','3-105',88.0),
('109','3-105',76.0),
('101','3-105',64.0),
('107','3-105',91.0),
('108','3-105',78.0),
('101','6-166',85.0),
('107','6-106',79.0),
('108','6-166',81.0);

/*Table structure for table `students` */

DROP TABLE IF EXISTS `students`;

CREATE TABLE `students` (
  `sno` varchar(3) NOT NULL,
  `sname` varchar(4) NOT NULL,
  `ssex` varchar(2) NOT NULL,
  `sbirthday` datetime DEFAULT NULL,
  `class` varchar(5) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `students` */

insert  into `students`(`sno`,`sname`,`ssex`,`sbirthday`,`class`) values 
('108','曾华','男','1977-09-01 00:00:00','95033'),
('105','匡明','男','1975-10-02 00:00:00','95031'),
('107','王丽','女','1976-01-23 00:00:00','95033'),
('101','李军','男','1976-02-20 00:00:00','95033'),
('109','王芳','女','1975-02-10 00:00:00','95031'),
('103','陆君','男','1974-06-03 00:00:00','95031');

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
