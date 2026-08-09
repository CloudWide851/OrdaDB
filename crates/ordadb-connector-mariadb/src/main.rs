use ordadb_connector_sdk::connector_pipe_argument;

#[tokio::main]
async fn main() {
    let result = async {
        let pipe = connector_pipe_argument()?;
        ordadb_connector_mysql::run_mariadb_helper(&pipe).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
