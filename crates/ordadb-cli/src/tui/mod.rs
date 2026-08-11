mod agent;
mod app;
mod native;
mod runtime;
mod settings;
mod terminal;
mod view;

use ordadb_types::Result;

pub async fn run() -> Result<()> {
    runtime::run().await
}
