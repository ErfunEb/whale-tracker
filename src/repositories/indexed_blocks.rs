use sqlx::mysql::MySqlPoolOptions;
use std::error::Error;

#[derive(Clone)]
pub struct IndexedBlocksRepository {
    pool: sqlx::MySqlPool,
}

impl IndexedBlocksRepository {
    pub async fn new(database_url: &str) -> Result<Self, Box<dyn Error>> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn mark_block_indexed(&self, block_number: u64) -> Result<(), Box<dyn Error>> {
        sqlx::query(
            "INSERT INTO indexed_blocks (block_number) VALUES (?)
            ON DUPLICATE KEY UPDATE indexed_at = CURRENT_TIMESTAMP",
        )
        .bind(block_number)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_first_last_block_indexed(
        &self,
    ) -> Result<(Option<u64>, Option<u64>), Box<dyn Error>> {
        let result: Option<(Option<i64>, Option<i64>)> =
            sqlx::query_as("SELECT MIN(block_number), MAX(block_number) from indexed_blocks")
                .fetch_optional(&self.pool)
                .await?;

        let (min, max) = result.map_or((None, None), |(min, max)| {
            (min.map(|v| v as u64), max.map(|v| v as u64))
        });

        Ok((min, max))
    }

    pub async fn find_missing_blocks(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<(u64, u64)>, Box<dyn Error>> {
        let indexed_blocks: Vec<i64> = sqlx::query_scalar(
            "SELECT block_number FROM indexed_blocks
                 WHERE block_number BETWEEN ? AND ?
                 ORDER BY block_number",
        )
        .bind(start_block)
        .bind(end_block)
        .fetch_all(&self.pool)
        .await?;

        let mut gaps = Vec::new();
        let mut previous_block = start_block;

        let mut range = vec![start_block as i64 - 1];
        range.extend(indexed_blocks);
        range.push(end_block as i64 + 1);

        for block in range {
            if block as u64 > previous_block + 1 {
                gaps.push((previous_block + 1, block as u64 - 1));
            }

            previous_block = block as u64;
        }

        Ok(gaps)
    }
}
