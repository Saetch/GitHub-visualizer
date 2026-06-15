
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let date = "2025-03-05";
    let hour = "12";

    let url = format!("https://data.gharchive.org/{date}-{hour}.json.gz");

    let response = reqwest::get(&url).await.unwrap();

    print!("{:?}", response);
}
