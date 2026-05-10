-- Bulk load generated CSVs (Grants/Projects/Documents schema)
-- 数据集路径：/var/lib/mysql-files/output_dir
-- 提速建议：启用 --local-infile=1，关闭 binlog/唯一检查/外键检查，分批 COMMIT。

USE tracking_grants_for_research;

SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS;
SET @OLD_UNIQUE_CHECKS      = @@UNIQUE_CHECKS;
SET @OLD_AUTOCOMMIT         = @@AUTOCOMMIT;
SET @OLD_SQL_LOG_BIN        = @@SQL_LOG_BIN;

SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET AUTOCOMMIT = 0;
SET SQL_LOG_BIN = 0;

-- 清空表（父表在后，子表在前）
TRUNCATE TABLE `Documents`;
TRUNCATE TABLE `Document_Types`;
TRUNCATE TABLE `Grants`;
TRUNCATE TABLE `Project_Outcomes`;
TRUNCATE TABLE `Project_Staff`;
TRUNCATE TABLE `Projects`;
TRUNCATE TABLE `Research_Staff`;
TRUNCATE TABLE `Research_Outcomes`;
TRUNCATE TABLE `Staff_Roles`;
TRUNCATE TABLE `Organisations`;
TRUNCATE TABLE `Organisation_Types`;
TRUNCATE TABLE `Tasks`;

-- 1. 维表 / 父表
LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Organisation_Types.csv'
REPLACE INTO TABLE `Organisation_Types`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`organisation_type`,`organisation_type_description`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Organisations.csv'
REPLACE INTO TABLE `Organisations`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`organisation_id`,`organisation_type`,`organisation_details`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Staff_Roles.csv'
REPLACE INTO TABLE `Staff_Roles`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`role_code`,`role_description`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Research_Outcomes.csv'
REPLACE INTO TABLE `Research_Outcomes`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`outcome_code`,`outcome_description`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Document_Types.csv'
REPLACE INTO TABLE `Document_Types`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`document_type_code`,`document_description`);
COMMIT;

-- 2. 主表
LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Research_Staff.csv'
REPLACE INTO TABLE `Research_Staff`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`staff_id`,`employer_organisation_id`,`staff_details`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Projects.csv'
REPLACE INTO TABLE `Projects`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`project_id`,`organisation_id`,`project_details`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Grants.csv'
REPLACE INTO TABLE `Grants`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`grant_id`,`organisation_id`,`grant_amount`,`grant_start_date`,`grant_end_date`,`other_details`);
COMMIT;

-- 3. 关联表
LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Project_Staff.csv'
REPLACE INTO TABLE `Project_Staff`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`staff_id`,`project_id`,`role_code`,`date_from`,`date_to`,`other_details`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Project_Outcomes.csv'
REPLACE INTO TABLE `Project_Outcomes`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`project_id`,`outcome_code`,`outcome_details`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Documents.csv'
REPLACE INTO TABLE `Documents`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`document_id`,`document_type_code`,`grant_id`,`sent_date`,`response_received_date`,`other_details`);
COMMIT;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Tasks.csv'
REPLACE INTO TABLE `Tasks`
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@task_id,@project_id,@task_details)
SET `task_id`=@task_id,
    `project_id`=@project_id,
    `task_details`=@task_details,
    `eg Agree Objectives`=NULL;
COMMIT;

SET SQL_LOG_BIN = @OLD_SQL_LOG_BIN;
SET AUTOCOMMIT = @OLD_AUTOCOMMIT;
SET UNIQUE_CHECKS = @OLD_UNIQUE_CHECKS;
SET FOREIGN_KEY_CHECKS = @OLD_FOREIGN_KEY_CHECKS;