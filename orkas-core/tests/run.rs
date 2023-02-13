use std::env;

use orkas_core::{consts::SECOND, Log, LogList, OrkasConfig};
use tokio::time::sleep;
use tracing::info;

#[tokio::test]
async fn run() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "INFO,update=DEBUG");
    }

    tracing_subscriber::fmt::init();

    let node_1 = OrkasConfig::simple("127.0.0.1:0".parse().unwrap())
        .start()
        .await
        .unwrap();

    let handle_1 = node_1.handle();
    let node_2 = OrkasConfig::simple("127.0.0.1:0".parse().unwrap())
        .start()
        .await
        .unwrap();

    let addr_2 = node_2.local_addr();
    let handle_2 = node_2.handle();

    info!(addr = %addr_2, "Node 2");

    sleep(2 * SECOND).await;

    handle_2.new_topic("test".to_owned()).unwrap();

    info!("joining node 1 to node 2");

    handle_1.join_one("test".to_owned(), addr_2).await.unwrap();

    for i in 100..110 {
        handle_1.log("test", Log::new(format!("#{i}"))).unwrap();
    }

    info!("waiting for replication...");
    sleep(20 * SECOND).await;

    let a = handle_1
        .read("test", |e: &LogList| e.read::<Vec<_>>())
        .unwrap();

    let b = handle_2
        .read("test", |e: &LogList| e.read::<Vec<_>>())
        .unwrap();

    info!(?a);
    info!(?b);

    node_1.stop().await;
    node_2.stop().await;
}
