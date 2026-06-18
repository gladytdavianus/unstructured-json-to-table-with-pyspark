CREATE DATABASE IF NOT EXISTS yelp_db;
USE yelp_db;

-- Hapus tabel lama jika ada
DROP TABLE IF EXISTS yelp_reviews;

-- Buat tabel baru
CREATE TABLE IF NOT EXISTS yelp_reviews (
    review_id VARCHAR(255) NOT NULL PRIMARY KEY,
    user_id VARCHAR(255),
    business_id VARCHAR(255),
    stars INT,
    text TEXT,
    date VARCHAR(50),
    useful INT,
    funny INT,
    cool INT,
    extracted_at VARCHAR(50),
    source VARCHAR(100),
    text_length INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Buat index baru
CREATE INDEX idx_user_id ON yelp_reviews(user_id);
CREATE INDEX idx_business_id ON yelp_reviews(business_id);
CREATE INDEX idx_stars ON yelp_reviews(stars);

