use std::env;
use tokio::net::{ TcpListener, TcpStream };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use std::net::{ SocketAddr };
use tokio::process::Command;
use std::process::Stdio;
#[cfg(target_os = "windows")]
const SHELL: &str = "cmd.exe";
#[cfg(not(target_os = "windows"))]
const SHELL: &str = "bash";
async fn handle_socket(mut stream: TcpStream, remote_addr: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let (mut read_half, mut write_half) = stream.split();

    println!("open {} for remote {}", &SHELL, remote_addr);
    let mut process = match Command::new(SHELL)
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn() {
        Ok(p) => p,
        Err(e) => {
            println!("create process failed with {}", e);
            return Ok(());
        }
    };
    let (mut stdout, mut stderr, mut stdin) = match (
        process.stdout.take(),
        process.stderr.take(),
        process.stdin.take(),
    ) {
        (Some(stdout), Some(stderr), Some(stdin)) => (stdout, stderr, stdin),
        _ => return Err("stdout, stderr or stdin take failed".into()),
    };
    loop {
        let mut stream_buf = [0;1024];
        let mut stdout_buf = [0;1024];
        let mut stderr_buf = [0;1024];
        tokio::select! {
            result = read_half.read(&mut stream_buf) => {
                match result {
                    Ok(0) => {
                        println!("stream read 0B");
                        return Ok(());
                    },
                    Ok(n) => {
                        println!("stream read {}B", &n);
                        if let Err(e) = stdin.write(&stream_buf[..n]).await {
                            println!("process write error");
                            return Err(Box::new(e));
                        }
                    },
                    Err(e) => {
                        println!("stream error");
                        return Err(Box::new(e));
                    }
                }
            },
            result = stdout.read(&mut stdout_buf) => {
                match result {
                    Ok(0) => {
                        println!("stdout read 0B");
                        return Ok(());
                    },
                    Ok(n) => {
                        println!("stdout read {}B", &n);
                        if let Err(e) = write_half.write(&stdout_buf[..n]).await {
                            println!("stdout->stream write error");
                            return Err(Box::new(e));
                        }
                    },
                    Err(e) => {
                        println!("process error");
                        return Err(Box::new(e));
                    }
                }
            },
            result = stderr.read(&mut stderr_buf) => {
                match result {
                    Ok(0) => {
                        println!("stderr read 0B");
                        return Ok(());
                    }
                    Ok(n) => {
                        println!("stderr read {}B", &n);
                        if let Err(e) = write_half.write(&stderr_buf[..n]).await {
                            println!("stderr->stream write error");
                            return Err(Box::new(e));
                        }
                    },
                    Err(e) => {
                        println!("process error");
                        return Err(Box::new(e));
                    }
                }
            },
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = env::args()
        .nth(1)
        .and_then(|arg1| arg1.parse::<u16>().ok())
        .unwrap_or(9999);
    println!("use 0.0.0.0:{}", port);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (socket, addr) = listener.accept().await?;
        tokio::spawn(async move {
            match handle_socket(socket, &addr).await {
                Ok(_) => {
                    println!("connection to {} done", addr);
                },
                Err(e) => {
                    println!("connectoin to {} broken with {}", addr, e);
                }
            };
        });
    }
}
