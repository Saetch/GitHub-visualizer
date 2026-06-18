use chrono::TimeZone;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum GitEventMessage{
    Placeholder{
        location_x: f64,
        location_y: f64,
        event_description: String,
        time: chrono::DateTime<chrono::Utc>,
    }
}


