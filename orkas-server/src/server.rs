use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use color_eyre::Result;

async fn server() -> Result<()> {
    let app = Router::new().route("/ws", get(handler));

    Ok(())
}
async fn handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(ws)
}

async fn ws(mut socket: WebSocket) {
    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };

        if socket.send(msg).await.is_err() {
            // client disconnected
            return;
        }
    }
}
