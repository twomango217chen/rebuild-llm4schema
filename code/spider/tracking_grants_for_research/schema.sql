-- MySQL-friendly schema (no DBDataGen annotations)
-- DROP DATABASE IF EXISTS `tracking_grants_for_research`;


CREATE TABLE IF NOT EXISTS `Organisation_Types` (
  `organisation_type` VARCHAR(10) NOT NULL,
  `organisation_type_description` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`organisation_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Organisations` (
  `organisation_id` INT NOT NULL,
  `organisation_type` VARCHAR(10) NOT NULL,
  `organisation_details` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`organisation_id`),
  FOREIGN KEY (`organisation_type`) REFERENCES `Organisation_Types`(`organisation_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Research_Staff` (
  `staff_id` INT NOT NULL,
  `employer_organisation_id` INT NOT NULL,
  `staff_details` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`staff_id`),
  FOREIGN KEY (`employer_organisation_id`) REFERENCES `Organisations`(`organisation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Staff_Roles` (
  `role_code` VARCHAR(10) NOT NULL,
  `role_description` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`role_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Research_Outcomes` (
  `outcome_code` VARCHAR(10) NOT NULL,
  `outcome_description` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`outcome_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Projects` (
  `project_id` INT NOT NULL,
  `organisation_id` INT NOT NULL,
  `project_details` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`project_id`),
  FOREIGN KEY (`organisation_id`) REFERENCES `Organisations`(`organisation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Project_Staff` (
  `staff_id` INT NOT NULL,
  `project_id` INT NOT NULL,
  `role_code` VARCHAR(10) NOT NULL,
  `date_from` DATETIME,
  `date_to` DATETIME,
  `other_details` VARCHAR(255),
  PRIMARY KEY (`staff_id`, `project_id`),
  FOREIGN KEY (`project_id`) REFERENCES `Projects`(`project_id`),
  FOREIGN KEY (`role_code`) REFERENCES `Staff_Roles`(`role_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Project_Outcomes` (
  `project_id` INT NOT NULL,
  `outcome_code` VARCHAR(10) NOT NULL,
  `outcome_details` VARCHAR(255),
  PRIMARY KEY (`project_id`, `outcome_code`),
  FOREIGN KEY (`project_id`) REFERENCES `Projects`(`project_id`),
  FOREIGN KEY (`outcome_code`) REFERENCES `Research_Outcomes`(`outcome_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Grants` (
  `grant_id` INT NOT NULL,
  `organisation_id` INT NOT NULL,
  `grant_amount` DECIMAL(19,4) NOT NULL,
  `grant_start_date` DATETIME NOT NULL,
  `grant_end_date` DATETIME NOT NULL,
  `other_details` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`grant_id`),
  FOREIGN KEY (`organisation_id`) REFERENCES `Organisations`(`organisation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Document_Types` (
  `document_type_code` VARCHAR(10) NOT NULL,
  `document_description` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`document_type_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Documents` (
  `document_id` INT NOT NULL,
  `document_type_code` VARCHAR(10),
  `grant_id` INT NOT NULL,
  `sent_date` DATETIME NOT NULL,
  `response_received_date` DATETIME NOT NULL,
  `other_details` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`document_id`),
  FOREIGN KEY (`document_type_code`) REFERENCES `Document_Types`(`document_type_code`),
  FOREIGN KEY (`grant_id`) REFERENCES `Grants`(`grant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `Tasks` (
  `task_id` INT NOT NULL,
  `project_id` INT NOT NULL,
  `task_details` VARCHAR(255) NOT NULL,
  `eg Agree Objectives` ENUM('Y','N'),
  PRIMARY KEY (`task_id`),
  FOREIGN KEY (`project_id`) REFERENCES `Projects`(`project_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
