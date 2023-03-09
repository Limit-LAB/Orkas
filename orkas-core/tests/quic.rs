use color_eyre::Result;
use futures::{AsyncWriteExt, StreamExt};
use orkas_core::consts::SECOND;
use s2n_quic::{client::Connect, provider::tls, stream::BidirectionalStream, Client, Server};
use tap::Pipe;
use tokio::{select, time::sleep};
use tracing::info;

const SERVERNAME: &str = "localhost";

fn gen_cert() -> Result<(String, String)> {
    let cert = rcgen::generate_simple_self_signed(vec![SERVERNAME.to_string()])?;
    (cert.serialize_pem()?, cert.serialize_private_key_pem()).pipe(Ok)
}

fn tls() -> Result<(tls::default::Server, tls::default::Client)> {
    let (cert, key) = gen_cert()?;

    let s = s2n_quic::provider::tls::default::Server::builder()
        .with_certificate(cert.clone(), key)?
        .with_key_logging()?
        .build()?;

    let c = s2n_quic::provider::tls::default::Client::builder()
        .with_certificate(cert)?
        .with_key_logging()?
        .build()?;

    Ok((s, c))
}

#[tokio::test]
async fn test_quic() {
    tracing_subscriber::fmt().init();

    select! {
        res = inner() => res.unwrap(),
        _ = sleep(25 * SECOND) => {
            panic!("test quic timeout")
        }
    }
}

async fn inner() -> Result<()> {
    let (s, c) = tls()?;

    let mut server = Server::builder()
        .with_tls(s)?
        .with_io("127.0.0.1:0")?
        .start()
        .unwrap();

    let addr = server.local_addr()?;

    info!("addr: {}", addr);

    tokio::spawn(async move {
        while let Some(mut st) = server.accept().await {
            info!("accepted");

            while let Ok(Some(bi)) = st.accept_bidirectional_stream().await {
                info!("Bi Stream established");

                let mut bi: BidirectionalStream = bi;

                while let Some(Ok(x)) = bi.next().await {
                    info!("Read: {}", String::from_utf8_lossy(&x));
                }
            }
        }
    });

    info!("connecting to {}", addr);

    let client: Client = Client::builder()
        .with_io("127.0.0.1:0")?
        .with_tls(c)?
        .start()
        .unwrap();

    let mut con = client
        .connect(Connect::new(addr).with_server_name(SERVERNAME))
        .await?;

    let mut bi = con.open_bidirectional_stream().await?;

    info!("opened");

    for _ in 0..20 {
        let num = rand::random::<u64>().to_string().into_bytes();
        bi.write_all(&num).await?;
        tokio::time::sleep(SECOND).await;
    }

    Ok(())
}
