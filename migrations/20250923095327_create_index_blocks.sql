CREATE TABLE indexed_blocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    block_number BIGINT NOT NULL UNIQUE,
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_indexed_blocks_block_number ON indexed_blocks(block_number);
