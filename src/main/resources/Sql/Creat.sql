create database spark;
use spark;

CREATE TABLE user_behavior (
                                   user_id INT,
                                   item_id INT,
                                   category_id INT,
                                   behavior_type VARCHAR(10),
                                   timestamp VARCHAR(30)
);