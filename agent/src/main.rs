/*               IRIS AGENT                    */
/*                                        XXX  */
/*               XXXXXXXXXXX             XXXXX */
/*          XXXXXXXXXXXXXXXXXXX           XXX  */
/*        XXXXXXXXXXXXXXXXXXXXXX               */
/*      XXXXXXXXX           XXXX               */
/*     XXXXXXX                      XX         */
/*    XXXXXX                        XXX        */
/*   XXXXX         XXXXXXX         XXXXX       */
/*  XXXXX       XXXXXXXXXXXXX       XXXXX      */
/* XXXXX       XXXXXXXX    XXX       XXXXX     */
/* XXXXX      XXXXXXXX      XXX      XXXXX     */
/* XXXXX      XXXXXXXXX    XXXX      XXXXX     */
/* XXXXX      XXXXXXXXXXXXXXXXX      XXXXX     */
/* XXXXX       XXXXXXXXXXXXXXX       XXXXX     */
/*  XXXXX       XXXXXXXXXXXXX       XXXXX      */
/*   XXXXX         XXXXXXX         XXXXX       */
/*    XXXXXX                     XXXXXX        */
/*     XXXXXXX                 XXXXXXX         */
/*      XXXXXXXXX           XXXXXXXXX          */
/*        XXXXXXXXXXXXXXXXXXXXXXXXX            */
/*          XXXXXXXXXXXXXXXXXXXXX              */
/*               XXXXXXXXXXX                   */
mod info;
use reqwest::{get, ClientBuilder};
use serde::{Deserialize, Serialize};

mod crypt;
mod screen;
mod stream;
use std::{net::IpAddr, string, time::Duration};

use crate::screen::takescreen;
const WS: &str = "wss://127.0.0.1:6061";
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
pub struct Memory {
    pub(crate)  total: u64,
    pub(crate)  free: u64,
    pub(crate)  used: u64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct tempcomponets{
    pub(crate) label: String,
    pub(crate) temperature: String,

}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Disktoosend {
    
    pub(crate) kind: String,
    pub(crate) file_system:String,
    pub(crate) name:String,
    pub(crate) free: u64,
    pub(crate) total: u64,
    pub(crate) usage: u64,
    pub(crate) read_only: bool,
    pub(crate) removable: bool,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Diskstoosend {
    pub(crate) disks: Vec<Disktoosend>
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
    
    let command_clone = command.clone();
    
    let command_clone2 = command_clone.to_string();
    
    if command_clone2.starts_with("stream"){
        
        let parts:Vec<&str> = command_clone.split(",").collect();
        if parts.len() != 3{
            
            let _ = add_result(url.to_string(), key.to_string(), "not an command".to_string(), taskid.to_string()).await;
        }
        else {
            let sesssion = parts[2].to_string();
            let tys = parts[1].to_string();
            let url_clone = WS.to_string();
            print!("Start handler");
            tokio::spawn(async move {
                let _ = stream::stream_handler(tys.to_string(), sesssion.to_string(), url_clone.to_string()).await; // offline handler
            });
        }
    }
    
    match command.as_str() {
        "ping" => {
            let _ = add_result(url.to_string(), key, "pong".to_string(), taskid.to_string()).await;
        }

        "get_version" => {
            let _ = add_result(url.to_string(), key, VERSION.to_string(), taskid.to_string()).await;
        }
        "screenshot" => {
            let screenbase64 = takescreen();
            let _ = add_result(url.to_string(), key, screenbase64, taskid.to_string()).await;
        }
        "memory" => {
            use sysinfo::{System};
            let sys = System::new_all();
            let total_memory = sys.total_memory() / 1024;
            let free_memory = sys.free_memory() / 1024;
            let used_memory = sys.used_memory() / 1024;
            let memory: Memory = Memory { total: total_memory, free: free_memory, used: used_memory };
            let memory = match serde_json::to_string(&memory){
                Ok(str) => str,
                Err(_) => {
                    
                    log("Error: During Parsing Memory Data").await;
                    let _ = add_result(url.to_string(), key, "Error: During Parsing Memory Data".to_string(), taskid.to_string()).await;
                    return ;
                },
            };
            let _ = add_result(url.to_string(), key, memory.to_string(), taskid.to_string()).await;
            
        }
        "disk" => {
            use sysinfo::{Disks};
            let disks = Disks::new_with_refreshed_list();
            let mut disk_list: Vec<Disktoosend> = Vec::new();
            
            for disk in &disks {
                let return_disk: Disktoosend = Disktoosend { kind: disk.kind().to_string(), file_system: disk.file_system().to_ascii_uppercase().to_string_lossy().to_string(), 
                    name: disk.name().to_string_lossy().to_string(), free: disk.available_space() / 1024 / 1024, 
                    total: disk.total_space() / 1024 / 1024, 
                    usage: disk.usage().total_written_bytes / 1024 / 1024, 
                    read_only: disk.is_read_only(), 
                    removable: disk.is_removable(),}; 
                disk_list.push(return_disk);
            }
            let disklist = match serde_json::to_string(&disk_list){
                Ok(str) => str,
                Err(_) => {
                    
                    log("Error: During Parsing Memory Data").await;
                    let _ = add_result(url.to_string(), key, "Error: During Parsing Memory Data".to_string(), taskid.to_string()).await;
                    return ;
                },
            };
            let _ = add_result(url.to_string(), key, disklist.to_string(), taskid.to_string()).await;

        }
        "temperature" => {
            use sysinfo::{Components};
            let components = Components::new_with_refreshed_list();
            let mut comp_list: Vec<tempcomponets> = Vec::new();
            for component in &components {
                let temp = match component.temperature() {
                    Some(temp) => temp.to_string(),
                    None => "Error Retriving".to_string(),
                };
                let name = component.label();

                let tempcomp: tempcomponets = tempcomponets { label: name.to_string(), temperature: temp.to_string()};
                comp_list.push(tempcomp);
            }
            let disdt = match serde_json::to_string(&comp_list){
                Ok(str) => str,
                Err(_) => {
                    
                    log("Error: During Parsing Memory Data").await;
                    let _ = add_result(url.to_string(), key, "Error: During Parsing Memory Data".to_string(), taskid.to_string()).await;
                    return ;
                },
            };
     
            let _ = add_result(url.to_string(), key, disdt.to_string(), taskid.to_string()).await;

        }
        "🎅" => {
            let _ = "santa"; 
            let _ = add_result(url.to_string(), key, "you unlocked 1 santa gift".to_string(), taskid.to_string()).await;
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
            
            log("Error: During Parsing Advertising Data").await;
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
        Err(_) => {
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
    let _res = client
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
        Err(_) => {
            return "".to_string();
        },
    };
    return txt.to_string();
}


#[tokio::main]
async fn main() {
   

    println!("agent inilized");
    let server = SERVER.to_string();
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
