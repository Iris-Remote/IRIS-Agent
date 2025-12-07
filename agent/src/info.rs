use reqwest::ClientBuilder;
use sysinfo::System;
use std::time::Instant;

pub fn getusername() -> String{
      let username=  std::env::var("USERNAME")
    .or_else(|_| std::env::var("USER"))
    .unwrap_or_else(|_| "unknown".into());
    return username;
}
pub fn get_local_ip() -> String{
    let socket = match std::net::UdpSocket::bind("0.0.0.0:0"){
        Ok(soc) => soc,
        Err(err) => return "".to_string(),
    };
    match socket.connect("8.8.8.8:80"){
        Ok(_) => {
          let local_ip = match socket.local_addr(){
            Ok(ip) => ip.ip(),
            Err(_) => {
                return "".to_string();
            },
                  };
           return local_ip.to_string();
        },
        Err(_) => {
            return  "".to_string();
        },
    };


}

pub fn gethostname() -> String{
    match System::host_name() {
        Some(hostname) => {
                        return hostname;
            }
        None => {
            return "Error Retriving".to_string();
        }
    }
}
pub fn getosname() -> String{
    match System::name() {
        Some(hostname) => {
                        return hostname;
            }
        None => {
            return "Error Retriving".to_string();
        }
    }
}
pub fn getkernelversion() -> String{
    match System::kernel_version() {
        Some(hostname) => {
                        return hostname;
            }
        None => {
            return "Error Retriving".to_string();
        }
    }
}
pub fn getosversion() -> String{
    match System::os_version() {
        Some(hostname) => {
                        return hostname;
            }
        None => {
            return "Error Retriving".to_string();
        }
    }
}
pub fn getpcuptime() -> String{

    return System::uptime().to_string();
}
pub async fn getlatency(url:String) -> String{

    let start = Instant::now();
    let client = ClientBuilder::new()
        .danger_accept_invalid_certs(true) 
        .build().expect("ERROR");
    let resp = client
        .get(url.to_string())
        .send()
        .await;
    let resp = match resp {
        Ok(resp) => resp,
        Err(_) => {
            return "Error Retriving".to_string();
    },
    }; 
    let elapsed = start.elapsed();
    return elapsed.as_millis().to_string();
}