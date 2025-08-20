-- Create a new database for our project
CREATE DATABASE abinbev_india_logistics;

-- Create a specific user for our application to use with YOUR chosen password
CREATE USER 'project_user'@'localhost' IDENTIFIED BY 'Abinbev@2025';

-- Give this user all necessary permissions on our new database
GRANT ALL PRIVILEGES ON abinbev_india_logistics.* TO 'project_user'@'localhost';

-- Apply the permission changes
FLUSH PRIVILEGES;