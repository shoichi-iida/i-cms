CREATE DATABASE `i_cms`;

USE `i_cms`;

CREATE TABLE `tbl_setting` (
    `setting_key` VARCHAR(30) NOT NULL,
    `setting_value` VARCHAR(30) NULL,
    PRIMARY KEY (`setting_key`)
) COMMENT '設定情報';

CREATE TABLE `tbl_notice` (
    `id` VARCHAR(36) NOT NULL,
    `notice_text` TEXT NULL,
    PRIMARY KEY (`id`)
) COMMENT 'お知らせ情報';

CREATE TABLE `tbl_update` (
    `id` VARCHAR(36) NOT NULL,
    `update_date` VARCHAR(30) NULL,
    `update_text` TEXT NULL,
    PRIMARY KEY (`id`)
) COMMENT '更新情報';

CREATE TABLE `tbl_access_hist` (
    `access_date` DATE NOT NULL,
    `account_id` VARCHAR(30) NOT NULL,
    PRIMARY KEY (`access_date`, `account_id`)
) COMMENT 'アクセス履歴情報';

CREATE TABLE `tbl_operation_hist` (
    `operation_date` DATETIME NOT NULL,
    `operation_id` VARCHAR(32) NOT NULL,
    `account_id` VARCHAR(30) NOT NULL,
    `operation` VARCHAR(60) NOT NULL,
    `return_operation` VARCHAR(30) NOT NULL,
    `args` TEXT NULL,
    PRIMARY KEY (`operation_date`, `operation_id`)
) COMMENT '操作履歴情報';

CREATE TABLE `tbl_group` (
    `id` VARCHAR(36) NOT NULL,
    `name` VARCHAR(30) NOT NULL,
    `admin` BOOLEAN NULL DEFAULT '0',
    `manage_system` BOOLEAN NULL DEFAULT '0',
    `manage_sales` BOOLEAN NULL DEFAULT '0',
    PRIMARY KEY (`id`)
) COMMENT 'グループ情報';

CREATE TABLE `tbl_account` (
    `id` INTEGER NOT NULL AUTOINCREMENT,
    `user_id` VARCHAR(30) NOT NULL,
    `password` VARCHAR(30) NOT NULL,
    `name` VARCHAR(30) NOT NULL,
    `admin` BOOLEAN NULL DEFAULT '0',
    `is_active` BOOLEAN NULL DEFAULT '1',
    PRIMARY KEY (`id`)
) COMMENT 'ユーザ情報';

CREATE TABLE `tbl_account_token` (
    `id` INTEGER NOT NULL,
    `token` VARCHAR(50) NOT NULL,
    `expires_at` DATE NOT NULL,
    `revoked` BOOLEAN NULL DEFAULT '0',
    PRIMARY KEY (`id`, `token`),
    CONSTRAINT `FK_ACCOUNT_SETTINGS_TBL` FOREIGN KEY (`id`) REFERENCES `tbl_account` (`id`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT 'ユーザ設定情報';

CREATE TABLE `tbl_account_settings` (
    `id` VARCHAR(30) NOT NULL,
    `setting_key` VARCHAR(50) NOT NULL,
    `setting_value` VARCHAR(50) NOT NULL,
    PRIMARY KEY (`id`, `setting_key`),
    CONSTRAINT `FK_ACCOUNT_SETTINGS_TBL` FOREIGN KEY (`id`) REFERENCES `tbl_account` (`id`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT 'ユーザ設定情報';

CREATE TABLE `mst_auth` (
  `auth_id` varchar(36) NOT NULL DEFAULT uuid(),
  `auth_name` varchar(30) NOT NULL,
  `function` varchar(50) NOT NULL,
  `operation` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`auth_id`),
  UNIQUE KEY `UN_MST_AUTH` (`function`)
) COMMENT='権限マスタ';

CREATE TABLE `tbl_group_affiliation` (
    `group_id` VARCHAR(36) NOT NULL,
    `account_id` VARCHAR(30) NOT NULL,
    PRIMARY KEY (`group_id`, `account_id`),
    CONSTRAINT `FK_GROUP_ID` FOREIGN KEY (`group_id`) REFERENCES `tbl_group` (`id`) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT `FK_ACCOUNT_ID` FOREIGN KEY (`account_id`) REFERENCES `tbl_account` (`id`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT '所属情報';

CREATE TABLE `tbl_auth` (
    `id` VARCHAR(30) NOT NULL,
    `function` VARCHAR(50) NOT NULL,
    `auth_value` BOOLEAN NULL DEFAULT '0',
    PRIMARY KEY (`id`, `function`),
    CONSTRAINT `FK_AUTH_TBL` FOREIGN KEY (`id`) REFERENCES `tbl_account` (`id`) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT `FK_AUTH_MST` FOREIGN KEY (`function`) REFERENCES `mst_auth` (`function`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT 'ユーザ権限情報';

CREATE TABLE `tbl_group_auth` (
    `id` VARCHAR(36) NOT NULL,
    `function` VARCHAR(50) NOT NULL,
    `auth_value` BOOLEAN NULL DEFAULT '0',
    PRIMARY KEY (`id`, `function`),
    CONSTRAINT `FK_GRP_AUTH_TBL` FOREIGN KEY (`id`) REFERENCES `tbl_group` (`id`) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT `FK_GRP_AUTH_MST` FOREIGN KEY (`function`) REFERENCES `mst_auth` (`function`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT 'グループ権限情報';

CREATE TABLE `mst_menu_group` (
	`menu_group_id` VARCHAR(36) NOT NULL DEFAULT UUID(),
	`menu_group_name` VARCHAR(30) NOT NULL,
	`flg_disable` BOOLEAN NULL DEFAULT '0',
	`flg_admin` BOOLEAN NULL DEFAULT '0',
	`icon` VARCHAR(30),
	`text_color` VARCHAR(60) NOT NULL,
	`back_color` VARCHAR(60) NOT NULL,
	`sort_order` INT NOT NULL,
	PRIMARY KEY (`menu_group_id`)
) COMMENT 'メニューグループマスタ';

CREATE TABLE `mst_menu` (
	`menu_id` VARCHAR(36) NOT NULL DEFAULT UUID(),
	`menu_name` VARCHAR(30) NOT NULL,
	`action_name` VARCHAR(60) NOT NULL,
	`flg_disable` BOOLEAN NULL DEFAULT '0',
	`flg_admin` BOOLEAN NULL DEFAULT '0',
	`icon` VARCHAR(30),
	`text_color` VARCHAR(60) NOT NULL,
	`back_color` VARCHAR(60) NOT NULL,
	`sort_order` INT NOT NULL,
	PRIMARY KEY (`menu_id`)
) COMMENT 'メニューマスタ';

CREATE TABLE `mst_menu_relation` (
	`menu_group_id` VARCHAR(36) NOT NULL,
	`menu_id` VARCHAR(36) NOT NULL,
	PRIMARY KEY (`menu_group_id`, `menu_id`),
	CONSTRAINT `FK_MST_MENU_RELATION_GRP` FOREIGN KEY (`menu_group_id`) REFERENCES `mst_menu_group` (`menu_group_id`) ON UPDATE CASCADE ON DELETE CASCADE,
	CONSTRAINT `FK_MST_MENU_RELATION_ID` FOREIGN KEY (`menu_id`) REFERENCES `mst_menu` (`menu_id`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT 'メニュー権限マスタ';

CREATE TABLE `mst_menu_auth` (
	`menu_id` VARCHAR(36) NOT NULL,
	`auth_id` VARCHAR(36) NOT NULL,
	PRIMARY KEY (`menu_id`, `auth_id`),
	CONSTRAINT `FK_MST_MENU_ID` FOREIGN KEY (`menu_id`) REFERENCES `mst_menu` (`menu_id`) ON UPDATE CASCADE ON DELETE CASCADE,
	CONSTRAINT `FK_MST_MENU_AUTH` FOREIGN KEY (`auth_id`) REFERENCES `mst_auth` (`auth_id`) ON UPDATE CASCADE ON DELETE CASCADE
) COMMENT 'メニュー権限マスタ';
