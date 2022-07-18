/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50717
 Source Host           : localhost:3306
 Source Schema         : mydb

 Target Server Type    : MySQL
 Target Server Version : 50717
 File Encoding         : 65001

 Date: 22/09/2020 11:13:33
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for teachers
-- ----------------------------
DROP TABLE IF EXISTS `teachers`;
CREATE TABLE `teachers`  (
  `tno` varchar(3) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tname` varchar(4) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tsex` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tbirthday` datetime(0) NOT NULL,
  `prof` varchar(6) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `depart` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of teachers
-- ----------------------------
INSERT INTO `teachers` VALUES ('804', '李诚', '男', '1958-12-02 00:00:00', '副教授', '计算机系');
INSERT INTO `teachers` VALUES ('856', '张旭', '男', '1969-03-12 00:00:00', '讲师', '电子工程系');
INSERT INTO `teachers` VALUES ('825', '王萍', '女', '1972-05-05 00:00:00', '助教', '计算机系');
INSERT INTO `teachers` VALUES ('831', '刘冰', '女', '1977-08-14 00:00:00', '助教', '电子工程系');

SET FOREIGN_KEY_CHECKS = 1;
