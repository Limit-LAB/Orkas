use std::{convert::Infallible, net::SocketAddr, str::FromStr, time::Duration};

use orkas::{Actor, Log, LogList, Orkas, OrkasConfig};
use tap::Pipe;
use tokio::time::sleep;
use tracing::info;

#[tokio::test]
async fn test_run() {
    std::env::set_var(
        "RUST_LOG",
        "INFO,message=TRACE,swim.broadcast_handler=TRACE",
    );
    tracing_subscriber::fmt::init();

    let addr_1 = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    let addr_2 = SocketAddr::from_str("127.0.0.1:8001").unwrap();
    let node_1 = Orkas::start(OrkasConfig::simple(addr_1));
    let node_2 = Orkas::start(OrkasConfig::simple(addr_2));

    sleep(Duration::from_secs(1)).await;

    info!("Creating topic on node 2");

    node_2.new_topic("test".to_owned()).await.unwrap();

    info!("joining node 1 to node 2");

    node_1.join_one("test".to_owned(), addr_2).await.unwrap();

    let updated = node_1
        .update("test", |e: &LogList, a: Actor| -> Result<_, Infallible> {
            e.append(Log::new("oops"), a).pipe(Some).pipe(Ok)
        })
        .await
        .unwrap();

    assert!(updated);

    sleep(Duration::from_secs(3)).await;

    let len1 = node_1.read("test", |e: &LogList| e.len());

    let len2 = node_2.read("test", |e: &LogList| e.len());

    assert_eq!(len1, len2);
}
