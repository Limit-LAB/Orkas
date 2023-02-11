use std::{convert::Infallible, net::SocketAddr, str::FromStr, time::Duration};

use orkas::{Actor, Log, LogList, OrkasConfig};
use tap::Pipe;
use tokio::time::sleep;
use tracing::info;

#[tokio::test]
async fn test_run() {
    std::env::set_var("RUST_LOG", "INFO,orkas=DEBUG,outbound=DEBUG");
    tracing_subscriber::fmt::init();

    let node_1 = OrkasConfig::simple("127.0.0.1:0".parse().unwrap())
        .start()
        .await
        .unwrap();
    let node_2 = OrkasConfig::simple("127.0.0.1:0".parse().unwrap())
        .start()
        .await
        .unwrap();

    let addr_2 = node_2.local_addr();

    node_2.new_topic("test".to_owned()).unwrap();

    info!("joining node 1 to node 2");

    node_1.join_one("test".to_owned(), addr_2).await.unwrap();

    for i in 100..110 {
        let updated = node_1
            .update("test", |e: &LogList, a: Actor| -> Result<_, Infallible> {
                e.append(Log::new(format!("#{i}")), a).pipe(Some).pipe(Ok)
            })
            .await
            .unwrap();
        assert!(updated);
    }

    sleep(Duration::from_secs(5)).await;

    let a = node_1
        .read("test", |e: &LogList| e.read::<Vec<_>>())
        .unwrap();

    let b = node_2
        .read("test", |e: &LogList| e.read::<Vec<_>>())
        .unwrap();

    info!(?a);
    info!(?b);

    node_1.stop().await;
    node_2.stop().await;
}
