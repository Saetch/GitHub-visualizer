use chrono::TimeZone;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum GitEventMessage{
    Placeholder{
        event_description: String,
        time: chrono::DateTime<chrono::Utc>,
        iso2: String,
        country: String,
    }
}


