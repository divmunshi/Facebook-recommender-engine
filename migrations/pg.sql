CREATE TABLE users (
    user_id VARCHAR(255),
    age INT,
    lang VARCHAR(50),
    country VARCHAR(255),
    created_at TIMESTAMP,
    tag VARCHAR(255), 
    gender VARCHAR(255)
);

CREATE TABLE sessions (
    session_id VARCHAR(255),
    user_id VARCHAR(255),
    session_end TIMESTAMP,
    session_start TIMESTAMP,
    rec_count INT,
    duration INTERVAL
);

CREATE TABLE events (
    session_id VARCHAR(255),
    user_id VARCHAR(255),
    created_at TIMESTAMP, 
    event_type VARCHAR(255)
);

CREATE TABLE items (
    item_key VARCHAR(255),
    user_id VARCHAR(255),
    bucket_key VARCHAR(255),
    content_type VARCHAR(255),
    created_at TIMESTAMP,
    tag VARCHAR(255)
);

CREATE TABLE recommendations (
    session_id VARCHAR(255),
    user_id VARCHAR(255),
    engagement_duration INTERVAL,
    is_final BOOLEAN,
    order_in_session INT,
    sent_at TIMESTAMP, 
    recieved_at TIMESTAMP
);

CREATE TABLE requests (
    user_id VARCHAR(255),
    session_id VARCHAR(255),
    recieved_at TIMESTAMP
);

CREATE TABLE logs (
    user_id VARCHAR(255),
    session_id VARCHAR(255),
    evt_time TIMESTAMP, 
    event_type VARCHAR(255), 
    recommendation VARCHAR(255)
) PARTITION BY RANGE(evt_time);

CREATE INDEX idx_users_user_id ON users(user_id);
CREATE INDEX idx_users_created_at ON users(created_at);

CREATE INDEX idx_sessions_user_id ON sessions(user_id);

CREATE INDEX idx_items_item_id ON items(item_id);
CREATE INDEX idx_items_item_key ON items(item_key);
CREATE INDEX idx_items_user_id ON items(user_id);
CREATE INDEX idx_items_created_at ON items(created_at);

CREATE INDEX idx_recommendations_item_id ON recommendations(item_id);
CREATE INDEX idx_recommendations_session_id ON recommendations(session_id);
CREATE INDEX idx_recommendations_user_id ON recommendations(user_id);
CREATE INDEX idx_recommendations_sent_at ON recommendations(sent_at);